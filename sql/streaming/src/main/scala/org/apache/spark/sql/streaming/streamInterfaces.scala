package org.apache.spark.sql.streaming

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.streaming.dstream.DStream

trait StreamRelationProvider {
  /** Returns a new stream relation with the given parameters. */
  def createStreamRelation(streamSqlContext: StreamSQLContext, parameter: Map[String, String]):
    BaseStreamRelation
}

abstract class BaseStreamRelation extends BaseRelation {
  val sqlContext = streamSqlContext.localSqlContext

  def streamSqlContext: StreamSQLContext
}

abstract class StreamScan extends BaseStreamRelation {
  def buildScan(): DStream[Row]
}