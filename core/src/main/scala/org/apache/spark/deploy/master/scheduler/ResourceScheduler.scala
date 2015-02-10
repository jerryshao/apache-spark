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

package org.apache.spark.deploy.master.scheduler

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master._

private[spark] abstract class ResourceScheduler {

  def initialize(conf: SparkConf): Unit

  def master(): Master

  def enoughCores(queue: String, cores: Int): Boolean

  protected def requestCores(queue: String, cores: Int): Int

  protected def releaseCores(queue: String, cores: Int): Unit

  def setTotalCores(cores: Int): Unit

  def requestResourcesForApp(app: ApplicationInfo): Set[(WorkerInfo, Int)] = {
    if (master.spreadOutApps) {
      val usableWorkers = master.workers.toArray.filter(_.state == WorkerState.ALIVE)
        .filter(canUse(app, _)).sortBy(_.coresFree).reverse
      val numUsable = usableWorkers.length
      val assigned = new Array[Int](numUsable) // Number of cores to give on each node
      var toAssign = {
        val coresRequired = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
        requestCores(app.queue, coresRequired)
      }
      var pos = 0
      while (toAssign > 0) {
        if (usableWorkers(pos).coresFree - assigned(pos) > 0) {
          toAssign -= 1
          assigned(pos) += 1
        }
        pos = (pos + 1) % numUsable
      }
      usableWorkers.zip(assigned).toSet
    } else {
      val array = ArrayBuffer[(WorkerInfo, Int)]()
      for (worker <- master.workers if worker.coresFree > 0 && worker.state == WorkerState.ALIVE) {
        if (app.coresLeft > 0) {
          if (canUse(app, worker)) {
            val coresToUse = {
              val coresRequired = math.min(worker.coresFree, app.coresLeft)
              requestCores(app.queue, coresRequired)
            }
            if (coresToUse > 0) {
              array += ((worker, coresToUse))
            }
          }
          return array.toSet
        }
      }
      array.toSet
    }
  }

  def releaseResourcesForApp(app: ApplicationInfo): Unit = {
    releaseCores(app.queue, app.coresGranted)
  }

  def requestResourcesForDriver(driver: DriverInfo): Option[(WorkerInfo, Int)] = {
    val shuffledAliveWorkers = Random.shuffle(
      master.workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0

    var numWorkersVisited = 0
    while (numWorkersVisited < numWorkersAlive) {
      val worker = shuffledAliveWorkers(curPos)
      numWorkersVisited += 1
      if (worker.memoryFree >= driver.desc.mem
        && worker.coresFree >= driver.desc.cores
        && enoughCores(driver.queue, driver.desc.cores)) {
        val coresRequired = requestCores(driver.queue, driver.desc.cores)
        return Some((worker, coresRequired))
      }
      curPos = (curPos + 1) % numWorkersAlive
    }

    None
  }

  def releaseResourcesForDriver(driver: DriverInfo): Unit = {
    releaseCores(driver.queue, driver.desc.cores)
  }

  def releaseExecutorResources(exec: ExecutorDesc): Unit = {
    releaseCores(exec.application.queue, exec.cores)
  }

  /**
    * Can an app use the given worker? True if the worker has enough memory and we haven't already
    * launched an executor for the app on it (right now the standalone backend doesn't like having
    * two executors on the same worker).
    */
  private def canUse(app: ApplicationInfo, worker: WorkerInfo): Boolean = {
    worker.memoryFree >= app.desc.memoryPerSlave && !worker.hasExecutor(app)
  }
}

private[spark] class UnlimitedResourceScheduler(val master: Master) extends ResourceScheduler {

  def initialize(conf: SparkConf): Unit = { }

  def enoughCores(queue: String, cores: Int): Boolean = true

  protected def requestCores(queue: String, cores: Int): Int = cores

  protected def releaseCores(queue: String, cores: Int): Unit = { }

  def setTotalCores(cores: Int): Unit = { }
}
