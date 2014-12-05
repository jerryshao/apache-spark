package org.apache.spark.sql.streaming

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.dsl.ExpressionConversions
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

class StreamSQLContext(
    ssc: StreamingContext,
    localSqlContext: SQLContext)
  extends Logging with ExpressionConversions {

  self =>

  def this(ssc: StreamingContext) = this(ssc, new SQLContext(ssc.sparkContext))

  def streamingContext = this.ssc

  def sqlContext = this.localSqlContext

  protected[sql] lazy val streamingRuntimeAnalyzer = new RuleExecutor[LogicalPlan] {
    lazy val batches: Seq[Batch] = Seq(
      Batch("Convert Stream", Once, DStreamRDDGenerator))
  }

  protected[sql] lazy val streamingPreAnalyzer = new RuleExecutor[LogicalPlan] {
    val fixedPoint = FixedPoint(100)
    lazy val batches: Seq[Batch] = Seq(
      Batch("Resolve Source Stream", fixedPoint, SourceDStreamResolver))

    object SourceDStreamResolver extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = plan transform {
        case UnresolvedRelation(dbName, name, alias) =>
          localSqlContext.catalog.lookupRelation(dbName, name, alias)
      }
    }
  }

  // Streaming specific ddl parser.


  def createSchemaDStream[A <: Product : TypeTag](stream: DStream[A]) = {
    SparkPlan.currentContext.set(localSqlContext)
    val attributes = ScalaReflection.attributesFor[A]
    val schema = StructType.fromAttributes(attributes)
    val rowStream = stream.transform(rdd => RDDConversions.productToRowRdd(rdd, schema))
    new SchemaDStream(this, LogicalDStream(attributes, rowStream)(self))
  }

  def applySchema(rowStream: DStream[Row], schema: StructType): SchemaDStream = {
    val logicalPlan = LogicalDStream(schema.toAttributes, rowStream)(self)
    new SchemaDStream(this, logicalPlan)
  }

  def sql(sqlText: String): SchemaDStream = {
    if (localSqlContext.dialect == "sql") {
      new SchemaDStream(this, localSqlContext.parseSql(sqlText))
    } else {
      sys.error(s"Unsupported SQL dialect: ${localSqlContext.dialect}")
    }
  }

  def registerDStreamAsTempStream(stream: SchemaDStream, streamName: String): Unit = {
    localSqlContext.catalog.registerTable(None, streamName, stream.baseLogicalPlan)
  }

  def stream(streamName: String): SchemaDStream =
    new SchemaDStream(this, localSqlContext.catalog.lookupRelation(None, streamName))

  def dropTempStream(streamName: String): Unit = {
    localSqlContext.catalog.unregisterTable(None, streamName)
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
    streamSqlContext.registerDStreamAsTempStream(schemaStream, "test")
    val a = streamSqlContext.sql("select * from test where i > 6 and i < 9")

    a.foreachRDD(r => r.foreach(println))

    ssc.start()
    ssc.awaitTermination()
  }
}
