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

  def asMap(): Map[String, (Int, Option[String])] = resourcesMap

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[PreferredResources] &&
      obj.asInstanceOf[PreferredResources].resourcesMap == this.resourcesMap
  }

  override def hashCode(): Int = resourcesMap.hashCode()
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

