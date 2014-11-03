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

import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.Logging

abstract class MergeThread[T: ClassTag] extends Thread with Logging {

  private val numPending = new AtomicInteger(0)

  private val pendingToBeMerged = new mutable.Queue[Array[T]]()

  private var maxMergeFactor: Int = _

  private var closed = false

  def initialize(maxMergeFactor: Int): Unit = {
    this.maxMergeFactor = maxMergeFactor
  }

  def close(): Unit = synchronized {
    closed = true
    waitForMerge()
    interrupt()
  }

  def startMerge(toBeMerged: BlockingQueue[T]): Unit = {
    if (!closed) {
      numPending.incrementAndGet()

      val mergeFactor = getMergeFactor(toBeMerged.size(), maxMergeFactor)
      val blockGroup = new mutable.ArrayBuffer[T]
      (0 until mergeFactor).foreach { blockGroup += toBeMerged.take() }

      pendingToBeMerged.synchronized {
        pendingToBeMerged.enqueue(blockGroup.toArray)
        pendingToBeMerged.notifyAll()
      }
    }
  }

  def waitForMerge(): Unit = synchronized {
    while (numPending.get() > 0) {
      wait()
    }
  }

  override def run(): Unit = {
    while (true) {
      var toBeMerged: Array[T] = null
      try {
        pendingToBeMerged.synchronized {
          while (pendingToBeMerged.size <= 0) {
            pendingToBeMerged.wait()
          }
          toBeMerged = pendingToBeMerged.dequeue()
        }
        merge(toBeMerged)
      } catch {
        case e: InterruptedException =>
          numPending.set(0)
          return
        case t: Throwable =>
          numPending.set(0)
          throw t
      } finally {
        this.synchronized {
          numPending.decrementAndGet()
          notifyAll()
        }
      }
    }
  }

  private def getMergeFactor(numMerge: Int, maxMergeWidth: Int): Int = {
    if (numMerge > maxMergeWidth) {
      math.min(numMerge - maxMergeWidth + 1, maxMergeWidth)
    } else {
      numMerge
    }
  }

  def merge(toBeMerged: Array[T]): Unit
}
