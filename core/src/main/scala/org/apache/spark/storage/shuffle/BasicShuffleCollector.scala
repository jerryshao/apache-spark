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

import org.apache.spark.{MapOutputTracker, Logging, TaskContext, ShuffleDependency}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.{BlockManager, BlockObjectWriter, FileSegment, ShuffleBlockId}

private[spark]
class BasicShuffleCollector(blockManger: BlockManager)
  extends BlockShuffleCollector(blockManger) with Logging {

  protected var shuffleWriterGroup: Array[BlockObjectWriter] = _

  override def init(context: TaskContext, dep: ShuffleDependency[_, _]) {
    super.init(context, dep)

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

  def collect[K, V](key: K, value: V) {
    val bucketId = partitioner.getPartition(key)
    shuffleWriterGroup(bucketId).write((key, value).asInstanceOf[Product2[Any, Any]])
  }

  def flush() { }

  def close(isSuccess: Boolean): Option[MapStatus] = {
    try {
      if (isSuccess) {
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
    } catch {
      case e: Exception =>
        // Force all the file handler to close and re-throw the exception.
        if (shuffleWriterGroup != null) {
          shuffleWriterGroup.foreach(_.close())
          shuffleWriterGroup = null
        }
        throw e
    }
  }

  def getBlockLocation(id: ShuffleBlockId): FileSegment = {
    val file = blockManger.diskBlockManager.getFile(id)
    new FileSegment(file, 0, file.length())
  }

  def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    for (reduceId <- 0 until numOutputSplits) {
      val blockId = ShuffleBlockId(shuffleId, partitionId, reduceId)
      blockManger.diskBlockManager.getFile(blockId).delete()
    }

    true
  }
}
