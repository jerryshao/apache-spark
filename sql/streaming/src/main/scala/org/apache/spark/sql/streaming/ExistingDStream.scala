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

package org.apache.spark.sql.streaming

import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

case class LogicalDStream(output: Seq[Attribute], stream: DStream[Row])
    (val streamSqlContext: StreamSQLContext)
  extends LogicalPlan with MultiInstanceRelation {
  def children = Nil

  def newInstance() =
    LogicalDStream(output.map(_.newInstance()), stream)(streamSqlContext).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan) = plan match {
    case LogicalDStream(_, otherStream) => stream.streamId == otherStream.streamId
    case _ => false
  }
}

case class PhysicalDStream(output: Seq[Attribute], @transient stream: DStream[Row])()
  extends LeafNode {
  import PhysicalDStream._

  override def execute() = {
    assert(validTime != null)
    stream.getOrCompute(validTime).getOrElse(new EmptyRDD[Row](sparkContext))
  }
}

object PhysicalDStream {
  protected[streaming] var validTime: Time = null

  def setValidTime(time: Time): Unit = {
    if (validTime == null) {
      validTime = time
    } else if (validTime != time) {
      validTime = time
    } else {
    }
  }
}

