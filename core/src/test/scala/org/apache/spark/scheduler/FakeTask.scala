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

package org.apache.spark.scheduler

import java.util.Properties

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.resource.PreferredResources

class FakeTask(
    stageId: Int,
    partitionId: Int,
    prefLocs: Seq[TaskLocation] = Nil,
    serializedTaskMetrics: Array[Byte] =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),
    isBarrier: Boolean = false,
    preferredResources: PreferredResources = PreferredResources.EMPTY)
  extends Task[Int](stageId, 0, partitionId, new Properties, serializedTaskMetrics,
    isBarrier = isBarrier, preferredResources = preferredResources) {

  override def runTask(context: TaskContext): Int = 0
  override def preferredLocations: Seq[TaskLocation] = prefLocs
}

object FakeTask {
  /**
   * Utility method to create a TaskSet, potentially setting a particular sequence of preferred
   * locations for each task (given as varargs) if this sequence is not empty.
   */
  def createTaskSet(numTasks: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createTaskSet(numTasks, stageAttemptId = 0, prefLocs: _*)
  }

  def createTaskSet(numTasks: Int, stageAttemptId: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createTaskSet(numTasks, stageId = 0, stageAttemptId, prefLocs: _*)
  }

  def createTaskSet(numTasks: Int, stageId: Int, stageAttemptId: Int, prefLocs: Seq[TaskLocation]*):
  TaskSet = {
    if (prefLocs.size != 0 && prefLocs.size != numTasks) {
      throw new IllegalArgumentException("Wrong number of task locations")
    }
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, if (prefLocs.size != 0) prefLocs(i) else Nil)
    }
    new TaskSet(tasks, stageId, stageAttemptId, priority = 0, null)
  }

  def createShuffleMapTaskSet(
      numTasks: Int,
      stageId: Int,
      stageAttemptId: Int,
      prefLocs: Seq[TaskLocation]*): TaskSet = {
    if (prefLocs.size != 0 && prefLocs.size != numTasks) {
      throw new IllegalArgumentException("Wrong number of task locations")
    }
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new ShuffleMapTask(stageId, stageAttemptId, null, new Partition {
        override def index: Int = i
      }, prefLocs(i), new Properties,
        SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array())
    }
    new TaskSet(tasks, stageId, stageAttemptId, priority = 0, null)
  }

  def createBarrierTaskSet(numTasks: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createBarrierTaskSet(numTasks, stageId = 0, stageAttempId = 0, prefLocs: _*)
  }

  def createBarrierTaskSet(
      numTasks: Int,
      stageId: Int,
      stageAttempId: Int,
      prefLocs: Seq[TaskLocation]*): TaskSet = {
    if (prefLocs.size != 0 && prefLocs.size != numTasks) {
      throw new IllegalArgumentException("Wrong number of task locations")
    }
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, if (prefLocs.size != 0) prefLocs(i) else Nil, isBarrier = true)
    }
    new TaskSet(tasks, stageId, stageAttempId, priority = 0, null)
  }

  def createResourcePreferredTaskSet(numTasks: Int, prefResources: PreferredResources*): TaskSet = {
    createResourcePreferredTaskSet(numTasks, stageId = 0, stageAttemptId = 0, prefResources: _*)
  }

  def createResourcePreferredTaskSet(
      numTasks: Int,
      stageId: Int,
      stageAttemptId: Int,
      prefResources: PreferredResources*): TaskSet = {
    if (prefResources.nonEmpty && prefResources.size != numTasks) {
      throw new IllegalArgumentException("Wrong number of task preferred resources")
    }
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, Nil, preferredResources = prefResources(i))
    }
    new TaskSet(tasks, stageId, stageAttemptId, priority = 0, null)
  }
}
