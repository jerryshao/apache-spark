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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.dsl.ExpressionConversions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

class StreamSQLContext(
    ssc: StreamingContext,
    sqlContext: SQLContext)
  extends Logging with ExpressionConversions {

  self =>

  def this(ssc: StreamingContext) = this(ssc, new SQLContext(ssc.sparkContext))

  def localStreamingContext = this.ssc

  def localSqlContext = this.sqlContext

  protected[sql] def precompilePlan(streamPlan: LogicalPlan): LogicalPlan = {
    val analyzedPlan = sqlContext.analyzer(streamPlan)
    val optimizedPlan = sqlContext.optimizer(analyzedPlan)
    optimizedPlan
  }

  protected[sql] def executePlan(plan: LogicalPlan) = localSqlContext.executePlan(plan)

  // Add stream specific strategies to SQLContext
  sqlContext.extraStrategies = StreamStrategy :: Nil

  // Streaming specific ddl parser.
  protected[sql] val ddlParser = new StreamDDLParser { val streamSqlContext = self }

  protected[sql] def parseSql(sql: String): LogicalPlan = {
    ddlParser(sql).getOrElse(sqlContext.parseSql(sql))
  }

  def logicalPlanToStreamQuery(plan: LogicalPlan): SchemaDStream = new SchemaDStream(this, plan)

  def createSchemaDStream[A <: Product : TypeTag](stream: DStream[A]) = {
    SparkPlan.currentContext.set(sqlContext)
    val attributes = ScalaReflection.attributesFor[A]
    val schema = StructType.fromAttributes(attributes)
    val rowStream = stream.transform(rdd => RDDConversions.productToRowRdd(rdd, schema))
    new SchemaDStream(this, LogicalDStream(attributes, rowStream)(self))
  }

  def baseRelationToSchemaDStream(baseRelation: BaseStreamRelation): SchemaDStream = {
    logicalPlanToStreamQuery(LogicalRelation(baseRelation))
  }

  def applySchema(rowStream: DStream[Row], schema: StructType): SchemaDStream = {
    val logicalPlan = LogicalDStream(schema.toAttributes, rowStream)(self)
    new SchemaDStream(this, logicalPlan)
  }

  def sql(sqlText: String): SchemaDStream = new SchemaDStream(this, parseSql(sqlText))

  def registerDStreamAsTempStream(stream: SchemaDStream, streamName: String): Unit = {
    sqlContext.catalog.registerTable(None, streamName, stream.logicalPlan)
  }

  def stream(streamName: String): SchemaDStream =
    new SchemaDStream(this, sqlContext.catalog.lookupRelation(None, streamName))

  def dropTempStream(streamName: String): Unit = {
    sqlContext.catalog.unregisterTable(None, streamName)
  }
}

object Test {
  case class SingleInt(i: Int)
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[10]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Duration(10000))
    val streamSqlContext = new StreamSQLContext(ssc)
    import streamSqlContext._
    val rdd = sc.parallelize(1 to 10).map(i => SingleInt(i))
    val constantStream = new ConstantInputDStream(ssc, rdd)

    val schemaStream = streamSqlContext.createSchemaDStream(constantStream)
    //val a = schemaStream.where('i > 5).select('i)
    val a = streamSqlContext.sql(
      """CREATE TEMPORARY STREAM avro
        |USING org.apache.spark.sql.avro
        |OPTIONS (path "xxxx", abc "fdsa", fdsa "ewrq")
      """.stripMargin)
    a.foreachRDD(rdd => rdd.foreach(println))

  }
}
