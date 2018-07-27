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

package org.apache.spark.scheduler.resource

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.rdd.RDD

private[spark] class ResourcesPreference(private val resourcesMap: Map[String, (Int, Boolean)]) {

  def getResource(tpe: String): Option[(Int, Boolean)] = {
    resourcesMap.get(tpe)
  }

  def mergeOther(other: ResourcesPreference): ResourcesPreference = {
    ResourcesPreference.mergeResourcesPreferences(this, other)
  }

  def asMap(): Map[String, (Int, Boolean)] = resourcesMap

  def isSatisfied(executorResources: ExecutorResources): Boolean = {
    resourcesMap.forall { case (tpe, (num, canUseCpu)) =>
      val matchedResources = executorResources.getMatchedResources(tpe)
      val matchedResourcesNum = matchedResources.map(_._2.length).sum
      matchedResourcesNum >= num || canUseCpu
    }
  }

  def occupyBy(taskId: Long, executorResources: ExecutorResources): Unit = {
    resourcesMap.foreach { case (tpe, (num, _)) =>
      val matchedResources = executorResources.getMatchedResources(tpe)
      val matchedResourcesNum = matchedResources.map(_._2.length).sum
      if (matchedResourcesNum >= num) {
        var markedNum = 0
        for ((_, resArray) <- matchedResources; res <- resArray) {
          if (markedNum < num) {
            res.occupiedBy = taskId
            markedNum += 1
          }
        }
      }
    }
  }

}

private[spark] object ResourcesPreference {
  lazy val DEFAULT = new ResourcesPreference(Map.empty)

  def mergeResourcesPreferences(res: ResourcesPreference*): ResourcesPreference = {
    val resourcesMap = new mutable.HashMap[String, (Int, Boolean)]
    res.foreach { r =>
      r.asMap().foreach { case (tpe, (num, canUseCpu)) =>
        if (resourcesMap.contains(tpe)) {
          val newNum = if (resourcesMap(tpe)._1 > num) resourcesMap(tpe)._1 else num
          val newCanUseCpu = canUseCpu && resourcesMap(tpe)._2
          resourcesMap.put(tpe, (newNum, newCanUseCpu))
        } else {
          resourcesMap.put(tpe, (num, canUseCpu))
        }
      }
    }

    new ResourcesPreference(resourcesMap.toMap)
  }
}

class ResourcesPreferenceBuilder[T] private[spark](rdd: RDD[T]) {

  private[this] val resourcesMap = new mutable.HashMap[String, (Int, Boolean)]

  def require(resourceType: String, numPerTask: Int): ResourcesPreferenceBuilder[T] = {
    resourcesMap.put(regularize(resourceType), (numPerTask, false))
    this
  }

  def prefer(resourceType: String, numPerTask: Int): ResourcesPreferenceBuilder[T] = {
    resourcesMap.put(regularize(resourceType), (numPerTask, true))
    this
  }

  def build(): RDD[T] = {
    val resourcesPreference = new ResourcesPreference(resourcesMap.toMap)
    rdd.setPreferredResources(resourcesPreference)
    rdd
  }

  private def regularize(resourceType: String): String = {
    var lowcase = resourceType.toLowerCase(Locale.ROOT)
    if (!lowcase.startsWith("/")) {
      lowcase = "/" + lowcase
    }

    lowcase.stripSuffix("/").stripSuffix("*")
  }
}

