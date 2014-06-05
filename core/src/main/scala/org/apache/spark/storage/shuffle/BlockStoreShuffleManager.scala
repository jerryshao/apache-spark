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

import org.apache.spark.Logging
import org.apache.spark.storage.{FileSegment, ShuffleBlockId, BlockManager}
import org.apache.spark.util.Utils

class BlockStoreShuffleManager(val blockManager: BlockManager) extends ShuffleManager with Logging {

  val shuffleCollector: ShuffleCollector = {
    val clzName = blockManager.conf.get("spark.shuffle.collector",
      "org.apache.spark.storage.shuffle.BasicShuffleCollector")
    val clz = Class.forName(clzName, true, Utils.getContextOrSparkClassLoader)
    clz.getConstructor(classOf[BlockManager])
      .newInstance(blockManager)
      .asInstanceOf[ShuffleCollector]
  }
  assert(shuffleCollector.isInstanceOf[BlockStoreShuffleCollector],
    "Created ShuffleCollector should be inherited from BlockStoreShuffleCollector")

  val shuffleFetcher: ShuffleFetcher = {
    val clzName = blockManager.conf.get("spark.shuffle.fetcher",
      "org.apache.spark.storage.shuffle.BlockStoreShuffleFetcher")
    val clz = Class.forName(clzName, true, Utils.getContextOrSparkClassLoader)
    clz.newInstance().asInstanceOf[ShuffleFetcher]
  }

  private val blkShuffleCollector = shuffleCollector.asInstanceOf[BlockStoreShuffleCollector]

  def removeShuffle(shuffleId: Int): Boolean = {
    blkShuffleCollector.removeShuffle(shuffleId)
  }

  def getBlockLocation(id: ShuffleBlockId): FileSegment = {
    blkShuffleCollector.getBlockLocation(id)
  }

  def stop() {
    shuffleCollector.stop()
    shuffleFetcher.stop()
  }
}
