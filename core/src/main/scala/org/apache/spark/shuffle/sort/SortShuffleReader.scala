/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort

import java.io.{File, BufferedOutputStream, FileOutputStream}
import java.util.Comparator
import java.util.concurrent.{LinkedBlockingQueue, PriorityBlockingQueue}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.reflect.ClassTag

import org.apache.spark.{Logging, InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleReader, BaseShuffleHandle}
import org.apache.spark.storage._
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.MergeUtil

/**
 * SortShuffleReader merges and aggregates shuffle data that has already been sorted within each
 * map output block.
 *
 * As blocks are fetched, we store them in memory until we fail to acquire space from the
 * ShuffleMemoryManager. When this occurs, we merge the in-memory blocks to disk and go back to
 * fetching.
 *
 * InMemoryMerger is responsible for managing un-merged in-memory blocks to release the memory
 * when memory usage is beyond threshold, the merged data will be written to file for later
 * on-disk merge if necessary.
 *
 * OnDiskMerger is responsible for managing the merged on-disk blocks. OnDiskMerger will never
 * merge more than spark.shuffle.maxMergeWidth segments at a time.
 *
 * We want to avoid merging more blocks than we need to. Our last disk-to-disk merge may
 * merge fewer than maxMergeWidth blocks, as its only requirement is that, after it has been
 * carried out, <= maxMergeWidth blocks remain. E.g., if maxMergeWidth is 10, no more blocks
 * will come in, and we have 13 on-disk blocks, the optimal number of blocks to include in the
 * last disk-to-disk merge is 4.
 *
 * While blocks are still coming in, we don't know the optimal number, so we hold off until we
 * either receive the notification that no more blocks are coming in, or until maxMergeWidth
 * merge is required no matter what.
 *
 * E.g. if maxMergeWidth is 10 and we have 19 or more on-disk blocks, a 10-block merge will put us
 * at 10 or more blocks, so we might as well carry it out now.
 *
 * The final iterator that is passed to user code merges this
 * on-disk iterator with the in-memory blocks that have not yet been spilled.
 */
private[spark] class SortShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext)
  extends ShuffleReader[K, C] with Logging {

  import SortShuffleReader._

  require(endPartition == startPartition + 1,
    "Sort shuffle currently only supports fetching one partition")

  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf
  private val ser = Serializer.getSerializer(dep.serializer)
  private val blockManager = SparkEnv.get.blockManager
  private val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024
  private val onDiskMergeWidth = conf.getInt("spark.shuffle.maxMergeWidth", 100)
  private val inMemoryMergeWidth = Int.MaxValue
  private val threadId = Thread.currentThread().getId

  /** An iterator for fetching raw shuffle blocks */
  private var shuffleRawBlockFetcherItr: ShuffleRawBlockFetcherIterator = _

  /** Array of in-memory shuffle blocks for merging */
  private val inMemoryBlocks = new LinkedBlockingQueue[MemoryShuffleBlock]()

  /** Array of on-disk shuffle blocks for merging */
  private val onDiskBlocks = new PriorityBlockingQueue[DiskShuffleBlock]()

  /** Key comparator for sort-merging, if keyOrdering is not defined,
    * using hashcode of key for comparison */
  private val keyComparator: Comparator[K] = dep.keyOrdering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K) = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  /** In memory merge thread to merge in-memory shuffle blocks when memory usage is beyond
    * threshold */
  private val inMemoryMerger = new ShuffleBlockMerger[MemoryShuffleBlock]

  /** on disk merge thread to merge on-disk shuffle blocks when block number is beyond
    * threshold */
  private val onDiskMerger = new ShuffleBlockMerger[DiskShuffleBlock]

  private final class ShuffleBlockMerger[T <: ShuffleBlock : ClassTag] extends MergeThread[T] {

    def merge(blocksToBeMerged: Array[T]): Unit = {
      val (tmpBlkId, file) = blockManager.diskBlockManager.createTempShuffleBlock()
      val fos = new FileOutputStream(file)
      val bos = new BufferedOutputStream(fos, fileBufferSize)

      if (blocksToBeMerged.length > 1) {
        val itrGroup = shuffleBlocksToIterators(blocksToBeMerged.toSeq)
        val partialMergedItr =
          MergeUtil.mergeSort(itrGroup, keyComparator, dep.keyOrdering, dep.aggregator)
        blockManager.dataSerializeStream(tmpBlkId, bos, partialMergedItr, ser)
      } else {
        // If only 1 block is in the array, then directly write to file without merge.
        val buffer = shuffleBlocksToBytes(blocksToBeMerged.toSeq).head.nioByteBuffer()
        val channel = fos.getChannel()
        while (buffer.hasRemaining) {
          channel.write(buffer)
        }
        channel.close()
      }

      logInfo(s"Merge ${blocksToBeMerged.length} blocks into file: ${file.getName}")

      onDiskBlocks.add(DiskShuffleBlock(tmpBlkId, file, file.length()))
      releaseShuffleBlocks(blocksToBeMerged)
    }
  }

  override def read(): Iterator[Product2[K, C]] = {
    inMemoryMerger.initialize(inMemoryMergeWidth)
    inMemoryMerger.start()

    onDiskMerger.initialize(onDiskMergeWidth)
    onDiskMerger.start()

    for ((blockId, blockData) <- fetchRawBlocks()) {
      if (blockData.isEmpty) {
        throw new IllegalStateException(s"block $blockId is empty for unknown reason")
      }

      inMemoryBlocks.add(MemoryShuffleBlock(blockId, blockData.get, blockData.get.size()))

      // Try to fit block in memory. If this fails, merge in-memory blocks to disk.
      val blockSize = blockData.get.size
      val granted = shuffleMemoryManager.tryToAcquire(blockData.get.size)
      if (granted >= blockSize) {
        logInfo(s"Grant memory $granted for shuffle block $blockId")
      } else {
        logInfo(s"Granted memory $granted is not enough to store block $blockId, " +
          s"doing merge to release the memory")

        shuffleMemoryManager.release(granted)

        inMemoryMerger.startMerge(inMemoryBlocks)
        // We should wait until this merge is finished to release the memory
        inMemoryMerger.waitForMerge()
      }

      if (onDiskBlocks.size() >= onDiskMergeWidth * 2 - 1) {
        onDiskMerger.startMerge(onDiskBlocks)
      }

      shuffleRawBlockFetcherItr.currentResult = null
    }

    inMemoryMerger.waitForMerge()

    while (onDiskBlocks.size() > onDiskMergeWidth) {
      onDiskMerger.startMerge(onDiskBlocks)
    }

    onDiskMerger.waitForMerge()

    logDebug(s"${inMemoryBlocks.size()} memory blocks and ${onDiskBlocks.size()} disk blocks left " +
      s"for final merge")

    // Merge on-disk blocks with in-memory blocks to directly feed to the reducer.
    val finalBlockGroup =
      inMemoryBlocks.toArray(new Array[MemoryShuffleBlock](inMemoryBlocks.size())) ++
      onDiskBlocks.toArray(new Array[DiskShuffleBlock](onDiskBlocks.size()))

    val finalItrGroup = shuffleBlocksToIterators(finalBlockGroup.toSeq)
    val mergedItr =
      MergeUtil.mergeSort(finalItrGroup, keyComparator, dep.keyOrdering, dep.aggregator)

    inMemoryBlocks.clear()
    onDiskBlocks.clear()
    inMemoryMerger.close()
    onDiskMerger.close()

    // Release the in-memory block and on-disk file when iteration is completed.
    val completionItr = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      mergedItr, () => releaseShuffleBlocks(finalBlockGroup))

    new InterruptibleIterator(context, completionItr.map(p => (p._1, p._2)))
  }

  override def stop(): Unit = ???

  private def fetchRawBlocks(): Iterator[(BlockId, Option[ManagedBuffer])] = {
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(handle.shuffleId, startPartition)

    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]()
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }

    val blocksByAddress = splitsByAddress.toSeq.map { case (address, splits) =>
      val blocks = splits.map { s =>
        (ShuffleBlockId(handle.shuffleId, s._1, startPartition), s._2)
      }
      (address, blocks.toSeq)
    }

    shuffleRawBlockFetcherItr = new ShuffleRawBlockFetcherIterator(
      context,
      SparkEnv.get.blockTransferService,
      blockManager,
      blocksByAddress,
      conf.getLong("spark.reducer.maxMbInFlight", 48) * 1024 * 1024)

    val completionItr = CompletionIterator[
      (BlockId, Option[ManagedBuffer]),
      Iterator[(BlockId, Option[ManagedBuffer])]](shuffleRawBlockFetcherItr,
      () => context.taskMetrics.updateShuffleReadMetrics())

    new InterruptibleIterator[(BlockId, Option[ManagedBuffer])](context, completionItr)
  }

  private def shuffleBlocksToBytes(shuffleBlocks: Seq[ShuffleBlock]): Seq[ManagedBuffer] = {
    shuffleBlocks.map { _ match {
        case MemoryShuffleBlock(id, buf, _) => buf
        case DiskShuffleBlock(id, file, _) =>  blockManager.getBlockData(id)
      }
    }
  }

  private def shuffleBlocksToIterators(shuffleBlocks: Seq[ShuffleBlock])
      : Seq[Iterator[Product2[K, C]]] = {
    shuffleBlocks.map { b =>
      val itr = b match {
        case MemoryShuffleBlock(id, buf, _) =>
          blockManager.dataDeserialize(id, buf.nioByteBuffer(), ser)
        case DiskShuffleBlock(id, file, _) =>
          val blockData = blockManager.getBlockData(id)
          blockManager.dataDeserialize(id, blockData.nioByteBuffer(), ser)
      }
      itr.asInstanceOf[Iterator[Product2[K, C]]]
    }
  }

  private def releaseShuffleBlocks(shuffleBlocks: Array[_ <: ShuffleBlock]): Unit = {
    shuffleBlocks.foreach { _ match {
      case MemoryShuffleBlock(_, _, length) =>
        shuffleMemoryManager.release(length, threadId)
      case DiskShuffleBlock(_, file, _) =>
        try {
          file.delete()
        } catch {
          case e: Exception => logWarning(s"Unexpected errors when deleting file:" +
            s"${file.getAbsolutePath}", e)
        }
      }
    }
  }
}

object SortShuffleReader {
  /** ShuffleBlock to represent the block fetched from shuffle output */
   sealed trait ShuffleBlock extends Comparable[ShuffleBlock] {

    def blockId: BlockId

    def length: Long

    def compareTo(o: ShuffleBlock): Int = length.compareTo(o.length)
  }

  /** Manage the in-memory shuffle block and related blockId */
  case class MemoryShuffleBlock(blockId: BlockId, blockData: ManagedBuffer, length: Long)
    extends ShuffleBlock

  /** Manage the on-disk shuffle block and related file, file length */
  case class DiskShuffleBlock(blockId: BlockId, file: File, length: Long) extends ShuffleBlock
}
