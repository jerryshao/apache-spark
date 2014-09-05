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

import java.io.{BufferedOutputStream, FileOutputStream, File}
import java.nio.ByteBuffer
import java.util.Comparator
import java.util.concurrent.{CountDownLatch, TimeUnit, LinkedBlockingQueue}

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.{Logging, InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleReader, BaseShuffleHandle}
import org.apache.spark.shuffle.hash.BlockStoreShuffleFetcher
import org.apache.spark.storage.{ShuffleBlockId, BlockId, BlockManagerId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext)
  extends ShuffleReader[K, C] with Logging {

  require(endPartition == startPartition + 1,
    "Sort shuffle currently only supports fetching one partition")

  sealed trait ShufflePartition
  case class MemoryPartition(blockId: BlockId, blockData: ByteBuffer) extends ShufflePartition
  case class FilePartition(blockId: BlockId, mappedFile: File) extends ShufflePartition

  private val mergingGroup = new LinkedBlockingQueue[ShufflePartition]()
  private val mergedGroup = new LinkedBlockingQueue[ShufflePartition]()
  private var numSplits: Int = 0
  private val mergeFinished = new CountDownLatch(1)
  private val mergingThread = new MergingThread()
  private val tid = Thread.currentThread().getId

  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf
  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer)
  private val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager
  private val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()

  private val ioSortFactor = conf.getInt("spark.shuffle.ioSortFactor", 100)
  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

  private val keyComparator: Comparator[K] = dep.keyOrdering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K) = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      h1 - h2
    }
  })

  override def read(): Iterator[Product2[K, C]] = {
    if (!dep.mapSideCombine && dep.aggregator.isDefined) {
      val iter = BlockStoreShuffleFetcher.fetch(handle.shuffleId, startPartition, context, ser,
        readMetrics)
      new InterruptibleIterator(context,
        dep.aggregator.get.combineValuesByKey(iter, context))
    } else {
      sortShuffleRead()
    }
  }

  private def sortShuffleRead(): Iterator[Product2[K, C]] = {
    val rawBlockIterator = fetchRawBlock()

    mergingThread.setNumSplits(numSplits)
    mergingThread.setDaemon(true)
    mergingThread.start()

    for ((blockId, blockData) <- rawBlockIterator) {
      if (blockData.isEmpty) {
        throw new IllegalStateException(s"block $blockId is empty for unknown reason")
      }

      val amountToRequest = blockData.get.remaining()
      val granted = shuffleMemoryManager.tryToAcquire(amountToRequest)
      val shouldSpill = if (granted < amountToRequest) {
        shuffleMemoryManager.release(granted)
        logInfo(s"Grant memory $granted less than the amount to request $amountToRequest, " +
          s"spilling data to file")
        true
      } else {
        false
      }

      if (!shouldSpill) {
        mergingGroup.offer(MemoryPartition(blockId, blockData.get))
      } else {
        val (tmpBlockId, file) = blockManager.diskBlockManager.createTempBlock()
        val channel = new FileOutputStream(file).getChannel()
        while (blockData.get.remaining() > 0) {
          channel.write(blockData.get)
        }
        channel.close()
        mergingGroup.offer(FilePartition(tmpBlockId, file))
      }
    }

    mergeFinished.await()

    // Merge the final group for combiner to directly feed to the reducer
    // TODO. Do we need to clean the temp merged file?
    val finalMergedPartArray = mergedGroup.toArray(new Array[ShufflePartition](mergedGroup.size()))
    val finalIterGroup = getIteratorGroup(finalMergedPartArray)
    val mergedIter = if (dep.aggregator.isDefined) {
      ExternalSorter.mergeWithAggregation(finalIterGroup, dep.aggregator.get.mergeCombiners,
        keyComparator, dep.keyOrdering.isDefined)
    } else {
      ExternalSorter.mergeSort(finalIterGroup, keyComparator)
    }

    mergedGroup.clear()

    // Release the memory of this thread
    shuffleMemoryManager.releaseMemoryForThisThread()

    new InterruptibleIterator(context,
      mergedIter.asInstanceOf[Iterator[Product2[K, C]]].map(p => (p._1, p._2)))
  }

  override def stop(): Unit = ???

  private def fetchRawBlock(): Iterator[(BlockId, Option[ByteBuffer])] = {
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(handle.shuffleId, startPartition)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]()
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }
    val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(s => (ShuffleBlockId(handle.shuffleId, s._1, startPartition), s._2)))
    }
    blocksByAddress.foreach { case (_, blocks) =>
      blocks.foreach { case (_, len) => if (len > 0) numSplits += 1 }
    }
    logInfo(s"Fetch $numSplits partitions for $tid")

    val rawBlkFetcherIterator = blockManager.getMultipleBytes(blocksByAddress, readMetrics)
    val completionIter = CompletionIterator[
      (BlockId, Option[ByteBuffer]),
      Iterator[(BlockId, Option[ByteBuffer])]](rawBlkFetcherIterator, {
      context.taskMetrics.updateShuffleReadMetrics()
    })

    new InterruptibleIterator[(BlockId, Option[ByteBuffer])](context, completionIter)
  }

  private def getIteratorGroup(shufflePartGroup: Array[ShufflePartition])
      : Seq[Iterator[Product2[K, C]]] = {
     shufflePartGroup.map { part =>
      val iter = part match {
        case MemoryPartition(id, buf) =>
          // Release memory usage
          shuffleMemoryManager.release(buf.remaining(), tid)
          blockManager.dataDeserialize(id, buf, ser)
        case FilePartition(id, file) =>
          val blockData = blockManager.diskStore.getBytes(id).getOrElse(
            throw new IllegalStateException(s"cannot get data from block $id"))
          blockManager.dataDeserialize(id, blockData, ser)
      }
      iter.asInstanceOf[Iterator[Product2[K, C]]]
    }.toSeq
  }

  private class MergingThread extends Thread {
    private var isLooped = true
    private var leftTobeMerged = 0

    def setNumSplits(numSplits: Int) {
      leftTobeMerged = numSplits
    }

    override def run() {
      while (isLooped) {
        if (leftTobeMerged < ioSortFactor && leftTobeMerged > 0) {
          var count = leftTobeMerged
          while (count > 0) {
            val part = mergingGroup.poll(100, TimeUnit.MILLISECONDS)
            if (part != null) {
              mergedGroup.offer(part)
              count -= 1
              leftTobeMerged -= 1
            }
          }
        } else if (leftTobeMerged >= ioSortFactor) {
          val mergingPartArray = ArrayBuffer[ShufflePartition]()
          var count = if (numSplits / ioSortFactor > ioSortFactor) {
            ioSortFactor
          } else {
            val mergedSize = mergedGroup.size()
            val left = leftTobeMerged - (ioSortFactor - mergedSize - 1)
            if (left <= ioSortFactor) {
              left
            } else {
              ioSortFactor
            }
          }
          val countCopy = count

          while (count > 0) {
            val part = mergingGroup.poll(100, TimeUnit.MILLISECONDS)
            if (part != null) {
              mergingPartArray += part
              count -= 1
              leftTobeMerged -= 1
            }
          }

          // Merge the partitions
          val iterGroup = getIteratorGroup(mergingPartArray.toArray)
          val partialMergedIter = if (dep.aggregator.isDefined) {
            ExternalSorter.mergeWithAggregation(iterGroup, dep.aggregator.get.mergeCombiners,
              keyComparator, dep.keyOrdering.isDefined)
          } else {
            ExternalSorter.mergeSort(iterGroup, keyComparator)
          }
          // Write merged partitions to disk
          val (tmpBlockId, file) = blockManager.diskBlockManager.createTempBlock()
          val fos = new BufferedOutputStream(new FileOutputStream(file), fileBufferSize)
          blockManager.dataSerializeStream(tmpBlockId, fos, partialMergedIter, ser)
          logInfo(s"Merge $countCopy partitions and write into file ${file.getName}")

          mergedGroup.add(FilePartition(tmpBlockId, file))
        } else {
          val mergedSize = mergedGroup.size()
          if (mergedSize > ioSortFactor) {
            leftTobeMerged = mergedSize

            // Swap the merged group and merging group and do merge again,
            // since file number is still larger than ioSortFactor
            assert(mergingGroup.size() == 0)
            mergingGroup.addAll(mergedGroup)
            mergedGroup.clear()
          } else {
            assert(mergingGroup.size() == 0)
            isLooped = false
            mergeFinished.countDown()
          }
        }
      }
    }
  }
}
