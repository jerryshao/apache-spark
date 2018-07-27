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

import org.apache.spark.annotation.DeveloperApi

@DeveloperApi
trait ResourceInformation {

  def tpe: String

  def id: String = "N/A"

  def spec: String = "N/A"

  private[spark] var occupiedBy: Long = ResourceInformation.UNUSED

  override def toString: String = s"$tpe id: $id and spec: $spec"

  override def equals(obj: Any): Boolean = obj match {
    case that: ResourceInformation =>
      tpe == that.tpe && id == that.id && spec == that.spec
    case _ =>
      false
  }

  override def hashCode(): Int = {
    Seq(tpe, id, spec).map(_.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }
}

private[spark] object ResourceInformation {
  val UNUSED = -1L
}


trait ResourceDiscoverer {
  def discover(): Array[ResourceInformation]
}
