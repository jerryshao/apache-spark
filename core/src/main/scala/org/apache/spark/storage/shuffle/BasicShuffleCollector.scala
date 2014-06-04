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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConversions._

import org.apache.spark.{MapOutputTracker, Logging, TaskContext, ShuffleDependency}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.{BlockManager, BlockObjectWriter, FileSegment, ShuffleBlockId}
import org.apache.spark.util.{MetadataCleanerType, MetadataCleaner, TimeStampedHashMap}

private[spark]
class BasicShuffleCollector(blockManger: BlockManager)
  extends BlockStoreShuffleCollector(blockManger) with Logging {

  private val shuffleStates =
    new TimeStampedHashMap[ShuffleId, (Int, ConcurrentLinkedQueue[Int])]()

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  def getCollectorForMapTask: Collector = new BasicBlockCollector

  def stop() {
    metadataCleaner.cancel()
  }

  class BasicBlockCollector extends BlockStoreCollector {

    private var shuffleWriterGroup: Array[BlockObjectWriter] = _
    private var completedMapTasks: ConcurrentLinkedQueue[Int] = _

    override def init(context: TaskContext, dep: ShuffleDependency[_, _]) {
      super.init(context, dep)

      shuffleStates.putIfAbsent(shuffleId, (numOutputSplits, new ConcurrentLinkedQueue[Int]()))
      completedMapTasks = shuffleStates(shuffleId)._2

      shuffleWriterGroup = Array.tabulate(numOutputSplits) { bucketId =>
        val blockId = ShuffleBlockId(shuffleId, partitionId, bucketId)
        val blockFile = blockManger.diskBlockManager.getFile(blockId)

        // Because of previous failures, the shuffle file may already exist on this machine.
        // If so, remove it.
        if (blockFile.exists()) {
          if (blockFile.delete()) {
            logInfo(s"Removed existing shuffle file $blockFile")
          } else {
            logWarning(s"Failed to remove existing shuffle file $blockFile")
          }
        }

        blockManger.getDiskWriter(blockId, blockFile, serializer, fileBufferSize)
      }
    }

    def collect(pair: Product2[Any, Any]) {
      val bucketId = partitioner.getPartition(pair._1)
      shuffleWriterGroup(bucketId).write(pair)
    }

    def flush() { }

    def close(isSuccess: Boolean): Option[MapStatus] = {
      try {
        val ret = if (isSuccess) {
          val shuffleMetrics = new ShuffleWriteMetrics
          var totalBytes = 0L
          var totalTime = 0L
          val compressedSizes: Array[Byte] = shuffleWriterGroup.map { writer =>
            writer.commit()
            writer.close()
            val size = writer.fileSegment().length
            totalBytes += size
            totalTime += writer.timeWriting()
            MapOutputTracker.compressSize(size)
          }
          shuffleWriterGroup = null

          shuffleMetrics.shuffleBytesWritten = totalBytes
          shuffleMetrics.shuffleWriteTime = totalTime
          taskContext.taskMetrics.shuffleWriteMetrics = Some(shuffleMetrics)

          Option(new MapStatus(blockManger.blockManagerId, compressedSizes))
        } else {
          shuffleWriterGroup.foreach { writer =>
            writer.revertPartialWrites()
            writer.close()
          }
          shuffleWriterGroup = null
          None
        }

        completedMapTasks.add(partitionId)
        ret
      } catch {
        case e: Exception =>
          // Force all the file handler to close and re-throw the exception.
          if (shuffleWriterGroup != null) {
            shuffleWriterGroup.foreach(_.close())
            shuffleWriterGroup = null
            completedMapTasks.add(partitionId)
          }
          throw e
      }
    }
  }

  def getBlockLocation(id: ShuffleBlockId): FileSegment = {
    val file = blockManger.diskBlockManager.getFile(id)
    new FileSegment(file, 0, file.length())
  }

  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleStates.remove(shuffleId)
    cleaned
  }

  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean =
    shuffleStates.get(shuffleId) match {
      case Some(state) =>
        for (mapId <- state._2; reduceId <- 0 until state._1) {
          val blockId = new ShuffleBlockId(shuffleId, mapId, reduceId)
          blockManger.diskBlockManager.getFile(blockId).delete()
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
