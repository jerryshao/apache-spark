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

import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JArray, JValue}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.JsonProtocol

/**
 * :: DeveloperApi ::
 * Developer API to describe the information of resources, like GPU, FPGA and so on. This
 * information will be provided to TaskInfo, task could leverage such information to schedule
 * embedded jobs like MPI which requires additional resource information.
 *
 * @param tpe The type of resource, like "/cpu", "/gpu/k80", "/gpu/p100".
 * @param id The id of resource if provided, such as id of GPU card.
 * @param spec The detailed information of resource if provided, such as GPU spec, VRAM size
 *             and so on.
 */
@DeveloperApi
case class ResourceInformation(tpe: String, id: String = "N/A", spec: String = "N/A") {

  private[spark] var occupiedByTask: Long = ResourceInformation.UNUSED

  override def toString: String = s"$tpe id: $id, spec: $spec, is occupied by: $occupiedByTask"

  private[spark] def isMatchedBy(preferredType: String): Boolean = {
    // There's a case that prefix matching will lead to error, for example, if preferred type is
    // "/gpu/p100", and the type here is "/gpu/p1000", using prefix matching will get an error.
    // To avoid this issue, appending "/" to "/gpu/p100" to make sure if it is the parent type.
    preferredType == tpe || tpe.startsWith(preferredType + "/")
  }
}

private[spark] object ResourceInformation {
  val UNUSED: Long = -1L
  val RESERVED: Long = -2L

  val SUPPORTED_RESOURCES = Set("/gpu")

  private implicit val format = DefaultFormats

  def occupyBy(taskId: Long, resources: Array[ResourceInformation]): Unit = {
    resources.foreach { r =>
      if (r.occupiedByTask == RESERVED) {
        r.occupiedByTask = taskId
      }
    }
  }

  def occupiedBy(
      taskId: Long,
      resources: Array[ResourceInformation]): Array[ResourceInformation] = {
    resources.filter(_.occupiedByTask == taskId)
  }

  def clearBy(taskId: Long, resources: Array[ResourceInformation]): Unit = {
    resources.foreach { r =>
      if (r.occupiedByTask == taskId) {
        r.occupiedByTask = UNUSED
      }
    }
  }

  def availableResources(resources: Array[ResourceInformation]): Array[ResourceInformation] = {
    resources.filter(_.occupiedByTask == UNUSED)
  }

  def toJson(resources: Array[Array[ResourceInformation]]): String = {
    val jvalue = JArray { resources.map {
      r => JArray(r.map(JsonProtocol.resourceInformationToJson).toList) }.toList
    }
    compact(render(jvalue))
  }

  def fromJson(jsonString: String): Array[Array[ResourceInformation]] = {
    parse(jsonString).extract[List[JValue]]
      .map(_.extract[List[JValue]]
        .map(JsonProtocol.resourceInformationFromJson).toArray)
      .toArray
  }
}

