package org.apache.spark.sql.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.{CreateTableUsing, DDLParser}
import org.apache.spark.util.Utils

private[sql] abstract class StreamDDLParser extends DDLParser {

  def streamSqlContext: StreamSQLContext

  protected val STREAM = Keyword("STREAM")

  override protected lazy val ddl: Parser[LogicalPlan] = createStream

  protected lazy val createStream: Parser[LogicalPlan] =
    CREATE ~ TEMPORARY ~ STREAM ~> ident ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case streamName ~ provider ~ opts =>
        CreateTableUsing(streamName, provider, opts)
    }
}

private[sql] case class CreateStreamUsing(
    streamName: String,
    provider: String,
    options: Map[String, String],
    streamSqlContext: StreamSQLContext) extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val loader = Utils.getContextOrSparkClassLoader
    val clazz: Class[_] = try loader.loadClass(provider) catch {
      case cnf: java.lang.ClassNotFoundException =>
        try loader.loadClass(provider + ".DefaultSource") catch {
          case cnf: java.lang.ClassNotFoundException =>
            sys.error(s"Failed to load class for data source: $provider")
        }
    }
    val dataSource = clazz.newInstance().asInstanceOf[StreamRelationProvider]
    val relation = dataSource.createStreamRelation(streamSqlContext, options)

    val plan = streamSqlContext.baseRelationToSchemaDStream(relation)
    streamSqlContext.registerDStreamAsTempStream(plan, streamName)
    Seq.empty
  }
}
