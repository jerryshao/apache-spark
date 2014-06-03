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

import org.apache.spark.{SparkEnv, ShuffleFetcher}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.Utils

@DeveloperApi
private[spark]
trait ShuffleManager {

  /**
   * Get ShuffleCollector for map output collect.
   */
  def shuffleCollector: ShuffleCollector

  /**
   * Get ShuffleFetcher for reduce input fetch.
   */
  def shuffleFetcher: ShuffleFetcher

  /**
   * BlockManager for shuffle manager.
   */
  def blockManager: BlockManager

  /**
   * Clean the shuffle related data according to shuffle id.
   */
  def removeShuffle(shuffleId: Int): Boolean

  /**
   * Interface for stopping the ShuffleManager and cleaning the resources.
   */
  def stop()
}

private[spark]
object ShuffleManager {
  private lazy val shuffleManager = synchronized {
    val blockManager = SparkEnv.get.blockManager
    val conf = SparkEnv.get.conf
    val clsName = conf.get("spark.shuffle.manager",
      "org.apache.spark.storage.shuffle.BlockStoreShuffleManager")
    val cls = Class.forName(clsName, true, Utils.getContextOrSparkClassLoader)
    try {
      cls.getConstructor(classOf[BlockManager])
        .newInstance(blockManager)
        .asInstanceOf[ShuffleManager]
    } catch {
      case _: NoSuchMethodException =>
        cls.getConstructor().newInstance().asInstanceOf[ShuffleManager]
    }
  }

  def getShuffleManager = shuffleManager
}
