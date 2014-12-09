package org.apache.spark.sql.streaming

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sources.LogicalRelation

object StreamStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case LogicalDStream(output, stream) => PhysicalDStream(output, stream) :: Nil
    case l @ LogicalRelation(s: StreamScan) => PhysicalDStream(l.output, s.buildScan()) :: Nil
    case _ => Nil
  }
}
