package org.apache.spark.sql.streaming

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
    DStreamRDDGenerator.setValidTime(validTime)
    // Scan the streaming logic plan to convert streaming plan to specific RDD logic plan.
    val analyzedPlan = streamSqlContext.streamingRuntimeAnalyzer(preAnalyzedPlan)

    Some(new SchemaRDD(streamSqlContext.sqlContext, analyzedPlan))
  }

  private lazy val preAnalyzedPlan = streamSqlContext.streamingPreAnalyzer(baseLogicalPlan)

  private lazy val physicalStream = {
    val tmp = ArrayBuffer[DStream[Row]]()
    preAnalyzedPlan.foreach(plan =>
      plan match {
        case LogicalDStream(_, stream) => tmp += stream
        case _ => Unit
      }
    )
    tmp
  }

  /**
   * Returns the schema of this SchemaDStream (represented by a [[StructType]]]).
   */
  lazy val schema: StructType = preAnalyzedPlan.schema

  // Streaming Query DSL
  def select(exprs: Expression*): SchemaDStream = {
    val aliases = exprs.zipWithIndex.map {
      case (ne: NamedExpression, _) => ne
      case (e, i) => Alias(e, s"c$i")()
    }
    new SchemaDStream(streamSqlContext, Project(aliases, baseLogicalPlan))
  }

  def where(condition: Expression): SchemaDStream = {
    new SchemaDStream(streamSqlContext, Filter(condition, baseLogicalPlan))
  }

  def join(otherPlan: SchemaDStream,
      joinType: JoinType = Inner,
      on: Option[Expression] = None): SchemaDStream = {
    new SchemaDStream(streamSqlContext,
      Join(baseLogicalPlan, otherPlan.baseLogicalPlan, joinType, on))
  }

  def tableJoin(otherPlan: SchemaRDD,
      joinType: JoinType = Inner,
      on: Option[Expression] = None): SchemaDStream = {
    new SchemaDStream(streamSqlContext,
      Join(baseLogicalPlan, otherPlan.logicalPlan, joinType, on))
  }

  def orderBy(sortExprs: SortOrder*): SchemaDStream =
    new SchemaDStream(streamSqlContext, Sort(sortExprs, baseLogicalPlan))

  def limit(limitNum: Int): SchemaDStream =
    new SchemaDStream(streamSqlContext, Limit(Literal(limitNum), baseLogicalPlan))

  def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): SchemaDStream = {
    val aliasedExprs = aggregateExprs.map {
      case ne: NamedExpression => ne
      case e => Alias(e, e.toString)()
    }
    new SchemaDStream(streamSqlContext, Aggregate(groupingExprs, aliasedExprs, baseLogicalPlan))
  }
}
