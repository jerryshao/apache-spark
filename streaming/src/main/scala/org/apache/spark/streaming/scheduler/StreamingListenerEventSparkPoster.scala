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

package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.StreamingContext

/**
  * A proxy class to route the [[StreamingListenerEvent]] to Spark listener bus and be processed
  * by [[SparkListener]]. This is currently used to proxy [[StreamingListenerEvent]] to
  * [[EventLoggingListener]] to dump events HistoryServer.
  */
private[spark] class StreamingListenerEventSparkPoster(ssc: StreamingContext)
  extends StreamingListener {

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    routeToSparkListenerBus(receiverStarted)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    routeToSparkListenerBus(receiverError)
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    routeToSparkListenerBus(receiverStopped)
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    routeToSparkListenerBus(batchSubmitted)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    routeToSparkListenerBus(batchStarted)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    routeToSparkListenerBus(batchCompleted)
  }

  override def onOutputOperationStarted(
      outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    routeToSparkListenerBus(outputOperationStarted)
  }

  override def onOutputOperationCompleted(
      outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    routeToSparkListenerBus(outputOperationCompleted)
  }

  private def routeToSparkListenerBus(event: StreamingListenerEvent): Unit = {
    ssc.sc.listenerBus.post(event)
  }
}
