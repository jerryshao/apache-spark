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

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master._

object CapacityResourceScheduler {

  sealed abstract class QueueNode(
      val queueName: String,
      val capacityPredef: Float) {

    require(capacityPredef >= 0.0F && capacityPredef <= 1.0F)

    lazy val coresPredef: Int = (parent.map(_.coresPredef).get * capacityPredef).toInt

    var parent: Option[QueueNode] = None

    var children: Seq[QueueNode] = Nil

    def usedCores: Int

    private[scheduler] def acquireCores(cores: Int, withLimit: Boolean): Int

    private[scheduler] def releaseCores(cores: Int): Int

    def availableCores = coresPredef - usedCores

    def usedCapacity: Float = usedCores.toFloat / coresPredef

    def queuePath: String = parent.map(p => s"${p.queuePath}/$queueName").getOrElse(s"/$queueName")
  }

  class ParentQueue(
      override val queueName: String,
      override val capacityPredef: Float)
    extends QueueNode(queueName, capacityPredef) {

    def usedCores: Int = {
      var cores: Int = 0
      children.foreach { c => cores += c.usedCores }
      cores
    }

    private[scheduler] def acquireCores(cores: Int, withLimit: Boolean): Int = {
      var coresNeeded = cores
      var actualCoresAcquired = 0
      for (c <- children; if coresNeeded > 0) {
        val coresGotten = c.acquireCores(coresNeeded, withLimit)
        actualCoresAcquired += coresGotten
        coresNeeded -= coresGotten
      }

      actualCoresAcquired
    }

    private[scheduler] def releaseCores(cores: Int): Int = {
      var coresTobeReleased = cores
      var actualCoresReleased = 0
      for (c <- children; if coresTobeReleased > 0) {
        val coresReleased = c.releaseCores(coresTobeReleased)
        coresTobeReleased -= coresReleased
        actualCoresReleased += coresReleased
      }

      actualCoresReleased
    }
  }

  final class RootQueue(var totalCores: Int) extends ParentQueue("root", 1.0F) {
    override lazy val coresPredef = totalCores
  }

  final class LeafQueue(
      override val queueName: String,
      override val capacityPredef: Float,
      parentNode: QueueNode,
      softLimit: Float = 0.0F,
      hardLimit: Float = 0.0F)
    extends QueueNode(queueName, capacityPredef) {

    require(softLimit >= hardLimit, "soft limit must be larger than hard limit")

    private var coresUsed: Int = 0

    parent = Some(parentNode)

    override def usedCores: Int = coresUsed

    def requestCoresForScheduling(cores: Int): Int = {
      val coresGotten = acquireCores(cores, false)
      if (coresGotten >= cores) {
        cores
      } else {
        cores + requestCoresFromParent(parentNode, cores - coresGotten)
      }
    }

    def releaseCoresForScheduling(cores: Int): Unit = {
      val coresReleased = releaseCores(cores)
      if (coresReleased < cores) {
        coresReleased + releaseCoresToParent(parentNode, cores - coresReleased)
      }
    }

    private[scheduler] def acquireCores(cores: Int, withLimit: Boolean): Int = {
      val reservedCores = if (withLimit) {
        if (usedCapacity < (1.0F - softLimit)) {
          (coresPredef * (1.0F - softLimit - usedCapacity)).toInt
        } else if (usedCapacity >= (1.0 - softLimit) && usedCapacity < (1.0 - hardLimit)) {
          (coresPredef * (1.0F - hardLimit - usedCores)).toInt
        } else {
          0
        }
      } else {
        availableCores
      }

      if (reservedCores >= cores) {
        coresUsed += cores
        cores
      } else {
        coresUsed += reservedCores
        reservedCores
      }
    }

    private[scheduler] def releaseCores(cores: Int): Int = {
      if (coresUsed - cores < 0) {
        val tmp = coresUsed
        coresUsed = 0
        tmp
      } else {
        coresUsed -= cores
        cores
      }
    }

    private def requestCoresFromParent(parent: QueueNode, cores: Int): Int = {
      val coresAcquired = parent.acquireCores(cores, true)
      if (coresAcquired >= cores) {
        coresAcquired
      } else {
        coresAcquired + parent.parent.map(requestCoresFromParent(_, cores - coresAcquired))
          .getOrElse(0)
      }
    }

    private def releaseCoresToParent(parent: QueueNode, cores: Int): Int = {
      val coresReleased = parent.releaseCores(cores)
      if (coresReleased >= cores) {
        coresReleased
      } else {
        coresReleased + parent.parent.map(releaseCoresToParent(_, cores - coresReleased))
          .getOrElse(0)
      }
    }
  }
}

private[spark] class CapacityResourceScheduler(val master: Master) extends ResourceScheduler {

  import CapacityResourceScheduler._

  private val rootQueue = new RootQueue(0)
  private val capacityQueues = mutable.ArrayBuffer[QueueNode]()
  capacityQueues += rootQueue
  private val leafQueues = new mutable.HashMap[String, LeafQueue]()

  private val queueNameConf = "spark.deploy.scheduler.capacity.%s.queues"
  private val queueCapacityConf = "spark.deploy.scheduler.%s.capacity"
  private val queueSoftLimitConf = "spark.deploy.scheduler.%s.capacity.softLimit"
  private val queueHardLimitConf = "spark.deploy.scheduler.%s.capacity.hardLimit"

  override def initialize(conf: SparkConf): Unit = {
    val queues = new mutable.Queue[String]()
    if (conf.contains(queueNameConf.format(rootQueue.queueName))) {
      queues.enqueue(rootQueue.queueName)
    } else {
      val defaultQueue = new LeafQueue("default", 1.0F, rootQueue)
      capacityQueues += defaultQueue
    }

    while (queues.nonEmpty) {
      val parentName = queues.dequeue()
      val queueNames = conf.get(queueNameConf.format(parentName)).split(",")

      for (name <- queueNames) {
        val capacity =
          conf.getInt(queueCapacityConf.format(name), 100 / queueNames.length) / 100F
        val parentQueue = capacityQueues.find(_.queueName == parentName)

        val a = if (conf.contains(queueNameConf.format(name))) {
         val queue = new ParentQueue(name, capacity)
          queue.parent = parentQueue
          parentQueue.foreach(p => p.children = p.children ++ (queue :: Nil))
          queues.enqueue(name)
          queue
        } else {
          val softLimit = conf.getInt(queueSoftLimitConf.format(name), 0) / 100F
          val hardLimit = conf.getInt(queueHardLimitConf.format(name), 0) / 100F

          val queue = new LeafQueue(name, capacity, parentQueue.get, softLimit, hardLimit)
          parentQueue.foreach(p => p.children = p.children ++ (queue :: Nil))
          leafQueues.put(name, queue)
          queue
        }
        capacityQueues += a
      }
    }
  }

  def enoughCores(queue: String, cores: Int): Boolean =
    leafQueues(queue).availableCores >= cores

  protected def requestCores(queue: String, cores: Int): Int = {
    leafQueues(queue).requestCoresForScheduling(cores)
  }

  protected def releaseCores(queue: String, cores: Int): Unit = {
    leafQueues(queue).releaseCoresForScheduling(cores)
  }

  def setTotalCores(cores: Int): Unit = {
    rootQueue.totalCores = cores
  }
}
