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

package org.apache.spark.storage.shuffle

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.apache.spark.{MapOutputTracker, ShuffleDependency, TaskContext, Logging}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.{FileSegment, ShuffleBlockId, BlockObjectWriter, BlockManager}
import org.apache.spark.util.{MetadataCleanerType, MetadataCleaner, TimeStampedHashMap}
import org.apache.spark.util.collection.{PrimitiveVector, PrimitiveKeyOpenHashMap}

private[spark]
class ConsolidatedShuffleCollector(blockManager: BlockManager)
  extends BlockStoreShuffleCollector(blockManager) with Logging {
  import ConsolidatedShuffleCollector._

  private val shuffleStates = new TimeStampedHashMap[ShuffleId, ShuffleState]()

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  def getCollectorForMapTask(): Collector = new ConsolidatedBlockCollector()

  def stop() {
    metadataCleaner.cancel()
  }

  class ConsolidatedBlockCollector extends BlockStoreCollector {
    private var shuffleState: ShuffleState = _
    private var fileGroup: ShuffleFileGroup = _
    private var shuffleWriteGroup: Array[BlockObjectWriter] = _

    override def init(context: TaskContext, dep: ShuffleDependency[_, _]) {
      super.init(context, dep)

      shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numOutputSplits))
      shuffleState = shuffleStates(shuffleId)

      fileGroup = getUnusedFileGroup()
      shuffleWriteGroup = Array.tabulate(numOutputSplits) { bucketId =>
        val blockId = ShuffleBlockId(shuffleId, partitionId, bucketId)
        blockManager.getDiskWriter(blockId, fileGroup(bucketId), serializer, fileBufferSize)
      }
    }

    def collect(pair: Product2[Any, Any]) {
      val bucketId = partitioner.getPartition(pair._1)
      shuffleWriteGroup(bucketId).write(pair)
    }

    def flush() {}

    def close(isSuccess: Boolean): Option[MapStatus] = {
      try {
        val ret = if (isSuccess) {
          val shuffleMetrics = new ShuffleWriteMetrics
          var totalBytes = 0L
          var totalTime = 0L
          val compressedSizes: Array[Byte] = shuffleWriteGroup.map { writer =>
            writer.commit()
            writer.close()
            val size = writer.fileSegment().length
            totalBytes += size
            totalTime += writer.timeWriting()
            MapOutputTracker.compressSize(size)
          }
          fileGroup.recordMapOutput(partitionId, shuffleWriteGroup.map(_.fileSegment().offset))
          shuffleWriteGroup = null

          shuffleMetrics.shuffleBytesWritten = totalBytes
          shuffleMetrics.shuffleWriteTime = totalTime
          taskContext.taskMetrics.shuffleWriteMetrics = Some(shuffleMetrics)

          Option(new MapStatus(blockManager.blockManagerId, compressedSizes))
        } else {
          shuffleWriteGroup.foreach { writer =>
            writer.revertPartialWrites()
            writer.close()
          }
          shuffleWriteGroup = null
          None
        }

        recycleFileGroup(fileGroup)
        ret
      } catch {
        case e: Exception =>
          if (shuffleWriteGroup != null) {
            shuffleWriteGroup.foreach(_.close())
            shuffleWriteGroup = null
            recycleFileGroup(fileGroup)
          }
          throw e
      }
    }

    private def getUnusedFileGroup(): ShuffleFileGroup = {
      val fileGroup = shuffleState.unusedFileGroups.poll()
      if (fileGroup != null) fileGroup else newFileGroup()
    }

    private def newFileGroup(): ShuffleFileGroup = {
      val fileId = shuffleState.nextFileId.getAndIncrement()
      val files = Array.tabulate[File](numOutputSplits) { bucketId =>
        val filename = physicalFileName(shuffleId, bucketId, fileId)
        blockManager.diskBlockManager.getFile(filename)
      }
      val fileGroup = new ShuffleFileGroup(fileId, shuffleId, files)
      shuffleState.allFileGroups.add(fileGroup)
      fileGroup
    }

    private def recycleFileGroup(group: ShuffleFileGroup) {
      shuffleState.unusedFileGroups.add(group)
    }
  }

  /**
   * Returns the physical file segment in which the given BlockId is located.
   * This function should only be called if shuffle file consolidation is enabled, as it is
   * an error condition if we don't find the expected block.
   */
  def getBlockLocation(id: ShuffleBlockId): FileSegment = {
    // Search all file groups associated with this shuffle.
    val shuffleState = shuffleStates(id.shuffleId)
    for (fileGroup <- shuffleState.allFileGroups) {
      val segment = fileGroup.getFileSegmentFor(id.mapId, id.reduceId)
      if (segment.isDefined) { return segment.get }
    }

    throw new IllegalStateException("Failed to find shuffle block: " + id)
  }

  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleStates.remove(shuffleId)
    cleaned
  }

  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean =
    shuffleStates.get(shuffleId) match {
      case Some(state) =>
        for (fileGroup <- state.allFileGroups; file <- fileGroup.files) {
          file.delete()
        }
        logInfo("Deleted all files for shuffle " + shuffleId)
        true
      case None =>
        logInfo("Could not find files for shuffle " + shuffleId + " for deleting")
        false
  }

  private def cleanup(cleanupTime: Long) {
    shuffleStates.clearOldValues(cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }
}

private[spark]
object ConsolidatedShuffleCollector {

  /**
   * Contains all the state related to a particular shuffle. This includes a pool of unused
   * ShuffleFileGroups, as well as all ShuffleFileGroups that have been created for the shuffle.
   */
  private class ShuffleState(val numBuckets: Int) {
    val nextFileId = new AtomicInteger(0)
    val unusedFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()
    val allFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()
  }

  def physicalFileName(shuffleId: Int, bucketId: Int, fileId: Int) = {
    "merged_shuffle_%d_%d_%d".format(shuffleId, bucketId, fileId)
  }

  /**
   * A group of shuffle files, one per reducer.
   * A particular mapper will be assigned a single ShuffleFileGroup to write its output to.
   */
  private class ShuffleFileGroup(val shuffleId: Int, val fileId: Int, val files: Array[File]) {
    /**
     * Stores the absolute index of each mapId in the files of this group. For instance,
     * if mapId 5 is the first block in each file, mapIdToIndex(5) = 0.
     */
    private val mapIdToIndex = new PrimitiveKeyOpenHashMap[Int, Int]()

    /**
     * Stores consecutive offsets of blocks into each reducer file, ordered by position in the file.
     * This ordering allows us to compute block lengths by examining the following block offset.
     * Note: mapIdToIndex(mapId) returns the index of the mapper into the vector for every
     * reducer.
     */
    private val blockOffsetsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }

    def numBlocks = mapIdToIndex.size

    def apply(bucketId: Int) = files(bucketId)

    def recordMapOutput(mapId: Int, offsets: Array[Long]) {
      mapIdToIndex(mapId) = numBlocks
      for (i <- 0 until offsets.length) {
        blockOffsetsByReducer(i) += offsets(i)
      }
    }

    /** Returns the FileSegment associated with the given map task, or None if no entry exists. */
    def getFileSegmentFor(mapId: Int, reducerId: Int): Option[FileSegment] = {
      val file = files(reducerId)
      val blockOffsets = blockOffsetsByReducer(reducerId)
      val index = mapIdToIndex.getOrElse(mapId, -1)
      if (index >= 0) {
        val offset = blockOffsets(index)
        val length =
          if (index + 1 < numBlocks) {
            blockOffsets(index + 1) - offset
          } else {
            file.length() - offset
          }
        assert(length >= 0)
        Some(new FileSegment(file, offset, length))
      } else {
        None
      }
    }
  }
}
