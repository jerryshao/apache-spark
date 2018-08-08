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

package org.apache.spark.rdd.resource

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ResourceInformation

/**
 * A class combined of preferred resource information provided by user. Each preferred resource
 * information is combined by: resource type, resources per task and optional resource type. Spark
 * internally will honor this preferred resource information to do scheduling.
 *
 * Different RDDs in one stage may have different resource preferences, here provides a simple check
 * to handle the conflicts, we can improve later to offer sophisticate conflict resolving methods.
 */
private[spark] class PreferredResources(
    private val resourcesMap: Map[String, (Int, Option[String])]) extends Logging {

  private val orderedPreferredResources: Array[(String, Int, Option[String])] = {
    resourcesMap.toArray.map { case (tpe, (num, optional)) =>
      (tpe, num, optional)
    }.sortWith { case (l, r) =>
      hierarchicalCompareTo(l._1, r._1)
    }
  }

  def getResource(tpe: String): Option[(Int, Option[String])] = {
    resourcesMap.get(tpe)
  }

  def mergeOther(other: PreferredResources): PreferredResources = {
    if (this.resourcesMap.nonEmpty && other.resourcesMap.nonEmpty && this != other) {
      throw new IllegalStateException(s"Preferred resources specified in different RDDs have " +
        s"conflicts [${this.resourcesMap}] [${other.resourcesMap}], Spark currently doesn't " +
        "support specifying different preferred resources in one stage")
    }

    if (this.resourcesMap.isEmpty) {
      other
    } else {
      this
    }
  }

  def isSatisfied(availableResources: Array[ResourceInformation]): Boolean = {
    var index = 0
    var isSatisfied = true
    val unusedResources = ResourceInformation.availableResources(availableResources)
    val optionalResources = new mutable.ArrayBuffer[(String, Int)]()

    // 1. Check if primary resource is satisfied.
    while (index < orderedPreferredResources.length && isSatisfied) {
      val (preferredType, preferredNum, optional) = orderedPreferredResources(index)
      val matchedRes = unusedResources.filter { p =>
        p.isMatchedBy(preferredType) && p.occupiedByTask == ResourceInformation.UNUSED
      }

      if (matchedRes.length >= preferredNum) {
        matchedRes.take(preferredNum).foreach(_.occupiedByTask = ResourceInformation.RESERVED)
      } else if (optional.nonEmpty) {
        optionalResources.append((optional.get, preferredNum))
      } else {
        isSatisfied = false
      }
      index += 1
    }

    // 2. Check if optional resource is satisfied
    var isOptionalSatisfied = true
    if (isSatisfied && optionalResources.nonEmpty) {
      index = 0
      val orderedOptionalResources =
        optionalResources.sortWith { case (l, r) => hierarchicalCompareTo(l._1, r._1) }

      while (index < orderedOptionalResources.length && isOptionalSatisfied) {
        val (optionalType, optionalNum) = orderedOptionalResources(index)
        val matchedRes = unusedResources.filter { p =>
          p.isMatchedBy(optionalType) && p.occupiedByTask == ResourceInformation.UNUSED
        }

        if (matchedRes.length >= optionalNum) {
          matchedRes.take(optionalNum).foreach(_.occupiedByTask = ResourceInformation.RESERVED)
        } else {
          isOptionalSatisfied = false
        }
        index += 1
      }
    }

    isSatisfied = isSatisfied && isOptionalSatisfied
    if (!isSatisfied) {
      unusedResources.foreach(_.occupiedByTask = ResourceInformation.UNUSED)
    }

    isSatisfied
  }

  def asMap(): Map[String, (Int, Option[String])] = resourcesMap

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[PreferredResources] &&
      obj.asInstanceOf[PreferredResources].resourcesMap == this.resourcesMap
  }

  override def hashCode(): Int = resourcesMap.hashCode()

  private def hierarchicalCompareTo(l: String, r: String): Boolean = {
    if (l.startsWith(r + "/")) {
      true
    } else if (r.startsWith(l + "/")) {
      false
    } else {
      l.compareTo(r) < 0
    }
  }
}

private[spark] object PreferredResources {
  lazy val EMPTY = new PreferredResources(Map.empty)
}

/**
 * A preferred resources builder for RDD. This is used to run RDD with heterogeneous resource
 * requirement. In the builder, we supported prefix resource type specification, for example user
 * could specify "/gpu/\*" or "/gpu" to tell Spark that any kind of GPU resources is preferred
 * for task. Or user could specify "/gpu/k80" to describe a specific type of GPU.
 *
 * Currently we only support "/gpu" resources, we will refactor to unify different resource
 * requirements like CPU, FPGA and so on.
 */
class ResourcesPreferenceBuilder[T] private[spark](rdd: RDD[T]) {

  private[this] val resourcesMap = new mutable.HashMap[String, (Int, Option[String])]

  /**
   * Specify the required resource type with resource number per task.
   */
  def require(resourceType: String, numPerTask: Int): ResourcesPreferenceBuilder[T] = {
    resourcesMap.put(regularize(resourceType), (numPerTask, None))
    this
  }

  /**
   * Specify the preferred resource type with resource number per task. If the current resource
   * type cannot be satisfied, optional can be specified to choose different resources.
   */
  def prefer(
      resourceType: String,
      numPerTask: Int,
      optional: String): ResourcesPreferenceBuilder[T] = {
    resourcesMap.put(regularize(resourceType), (numPerTask, Some(regularize(optional))))
    this
  }

  /**
   * Build to generate preferred resource information and set into RDD.
   */
  def build(): RDD[T] = {
    val resourcesPreference = new PreferredResources(resourcesMap.toMap)
    rdd.setPreferredResources(resourcesPreference)
    rdd
  }

  private def regularize(resourceType: String): String = {
    var lowcase = resourceType.toLowerCase(Locale.ROOT)
    if (!lowcase.startsWith("/")) {
      lowcase = "/" + lowcase
    }

    lowcase.stripSuffix("*").stripSuffix("/")
  }
}

