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

import org.apache.spark.sql.sources.LogicalRelation

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

class SchemaDStream(
    val streamSqlContext: StreamSQLContext,
    val baseLogicalPlan: LogicalPlan)
  extends DStream[Row](streamSqlContext.streamingContext) {

  override def dependencies = physicalStream.toList

  override def slideDuration: Duration = physicalStream.head.slideDuration

  override def compute(validTime: Time): Option[RDD[Row]] = {
    // Set the valid batch duration for this rule to get correct RDD in DStream of this batch
    // duration
    PhysicalDStream.setValidTime(validTime)
    // Scan the streaming logic plan to convert streaming plan to specific RDD logic plan.
    Some(streamSqlContext.executePlan(preOptimizedPlan).toRdd)
  }

  protected[sql] val logicalPlan: LogicalPlan = baseLogicalPlan match {
    case _: Command =>
      val queryExecution = streamSqlContext.executePlan(baseLogicalPlan)
      // FIXME. null should be replaced as a normal DStream
      LogicalDStream(queryExecution.analyzed.output, null)(streamSqlContext)
    case _ =>
      baseLogicalPlan
  }

  private lazy val preOptimizedPlan = streamSqlContext.precompilePlan(logicalPlan)

  private lazy val physicalStream = {
    val tmp = ArrayBuffer[DStream[Row]]()
    preOptimizedPlan.foreach(plan =>
      plan match {
        case LogicalDStream(_, stream) => tmp += stream
        case LogicalRelation(s: StreamScan) => tmp += s.buildScan()
        case _ => Unit
      }
    )
    tmp
  }

  /**
   * Returns the schema of this SchemaDStream (represented by a [[StructType]]]).
   */
  lazy val schema: StructType = preOptimizedPlan.schema

  // Streaming Query DSL
  def select(exprs: Expression*): SchemaDStream = {
    val aliases = exprs.zipWithIndex.map {
      case (ne: NamedExpression, _) => ne
      case (e, i) => Alias(e, s"c$i")()
    }
    new SchemaDStream(streamSqlContext, Project(aliases, logicalPlan))
  }

  def where(condition: Expression): SchemaDStream = {
    new SchemaDStream(streamSqlContext, Filter(condition, logicalPlan))
  }

  def join(otherPlan: SchemaDStream,
      joinType: JoinType = Inner,
      on: Option[Expression] = None): SchemaDStream = {
    new SchemaDStream(streamSqlContext,
      Join(logicalPlan, otherPlan.logicalPlan, joinType, on))
  }

  def tableJoin(otherPlan: SchemaRDD,
      joinType: JoinType = Inner,
      on: Option[Expression] = None): SchemaDStream = {
    new SchemaDStream(streamSqlContext,
      Join(logicalPlan, otherPlan.logicalPlan, joinType, on))
  }

  def orderBy(sortExprs: SortOrder*): SchemaDStream =
    new SchemaDStream(streamSqlContext, Sort(sortExprs, logicalPlan))

  def limit(limitNum: Int): SchemaDStream =
    new SchemaDStream(streamSqlContext, Limit(Literal(limitNum), logicalPlan))

  def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): SchemaDStream = {
    val aliasedExprs = aggregateExprs.map {
      case ne: NamedExpression => ne
      case e => Alias(e, e.toString)()
    }
    new SchemaDStream(streamSqlContext, Aggregate(groupingExprs, aliasedExprs, logicalPlan))
  }
}
