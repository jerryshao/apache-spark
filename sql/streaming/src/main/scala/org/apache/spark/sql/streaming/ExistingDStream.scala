package org.apache.spark.sql.streaming

import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

object DStreamRDDGenerator extends Rule[LogicalPlan] {
  private var validTime: Time = _

  def setValidTime(time: Time): Unit = {
    if (validTime == null) {
      validTime = time
    } else if (validTime != time) {
      validTime = time
    } else {
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p @ LogicalDStream(output, stream) =>
      assert(validTime != null)
      stream.getOrCompute(validTime).map { rdd =>
        LogicalRDD(output, rdd)(p.streamSqlContext.sqlContext)
      }.getOrElse {
        LogicalRDD(output,
          new EmptyRDD[Row](p.streamSqlContext.streamingContext.sparkContext))(
            p.streamSqlContext.sqlContext)
      }
  }
}

case class LogicalDStream(output: Seq[Attribute], stream: DStream[Row])
  (val streamSqlContext: StreamSQLContext) extends LogicalPlan {
  override lazy val resolved = false
  def children = Nil
}


