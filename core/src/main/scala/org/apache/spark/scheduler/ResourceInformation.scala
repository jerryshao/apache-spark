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

import org.apache.spark.annotation.DeveloperApi

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

  override def toString: String = s"$tpe id: $id and spec: $spec"
}

private[spark] object ResourceInformation {
  val UNUSED = -1L
}

