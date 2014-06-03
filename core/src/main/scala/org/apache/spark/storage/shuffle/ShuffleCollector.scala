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

import org.apache.spark.{ShuffleDependency, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.MapStatus

@DeveloperApi
private[spark]
trait ShuffleCollector {

  def createCollector: Collector

  def stop()

  trait Collector {
    /**
     * Initialize the shuffle collector. AbstractShuffleCollector will implement
     * this interface to do basic initialization.
     */
    def init(context: TaskContext, dep: ShuffleDependency[_, _])

    /**
     * Collect map output key and value accordingly.
     */
    def collect[K, V](key: K, value: V)

    /**
     * Interface for flushing the collected data to destination.
     */
    def flush()

    /**
     * Interface for closing the shuffle collector.
     * @param isSuccess flag to specify whether the task is success or failed
     * @return MapStatus if task is success, otherwise return None
     */
    def close(isSuccess: Boolean): Option[MapStatus]
  }
}
