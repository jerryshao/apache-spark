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

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{FileSegment, ShuffleBlockId, BlockManager}

private[spark]
abstract class BlockShuffleCollector(blockManager: BlockManager) extends ShuffleCollector {
  type ShuffleId = Int

  val conf = blockManager.conf

  protected var partitionId: Int = _
  protected var shuffleId: Int = _
  protected var partitioner: Partitioner = _
  protected var numOutputSplits: Int = _
  protected var serializer: Serializer = _

  protected var taskContext: TaskContext = _
  protected var dep: ShuffleDependency[_, _] = _

  protected val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 100) << 10

  def init(context: TaskContext, dep: ShuffleDependency[_, _]) {
    this.partitionId = context.partitionId
    this.shuffleId = dep.shuffleId
    this.partitioner = dep.partitioner
    this.numOutputSplits = partitioner.numPartitions
    this.serializer = dep.serializer

    this.taskContext = context
    this.dep = dep
  }

  /**
   * Get the shuffle block location according to shuffle block id.
   */
  def getBlockLocation(id: ShuffleBlockId): FileSegment

  /**
   * Remove the shuffle blocks according to shuffle id.
   */
  def removeShuffleBlocks(shuffleId: ShuffleId): Boolean
}
