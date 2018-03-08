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

package org.apache.spark.sql.execution.streaming.sources

import java.io._
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.commons.io.IOUtils

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous.RateStreamContinuousReader
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousReader, MicroBatchReader, Offset}
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}
import org.apache.spark.util.{ManualClock, SystemClock}

object RateSourceProvider {
  val SCHEMA =
    StructType(StructField("timestamp", TimestampType) :: StructField("value", LongType) :: Nil)

  val VERSION = 1

  val NUM_PARTITIONS = "numPartitions"
  val ROWS_PER_SECOND = "rowsPerSecond"
  val RAMP_UP_TIME = "rampUpTime"

  /** Calculate the end value we will emit at the time `seconds`. */
  def valueAtSecond(seconds: Long, rowsPerSecond: Long, rampUpTimeSeconds: Long): Long = {
    // E.g., rampUpTimeSeconds = 4, rowsPerSecond = 10
    // Then speedDeltaPerSecond = 2
    //
    // seconds   = 0 1 2  3  4  5  6
    // speed     = 0 2 4  6  8 10 10 (speedDeltaPerSecond * seconds)
    // end value = 0 2 6 12 20 30 40 (0 + speedDeltaPerSecond * seconds) * (seconds + 1) / 2
    val speedDeltaPerSecond = rowsPerSecond / (rampUpTimeSeconds + 1)
    if (seconds <= rampUpTimeSeconds) {
      // Calculate "(0 + speedDeltaPerSecond * seconds) * (seconds + 1) / 2" in a special way to
      // avoid overflow
      if (seconds % 2 == 1) {
        (seconds + 1) / 2 * speedDeltaPerSecond * seconds
      } else {
        seconds / 2 * speedDeltaPerSecond * (seconds + 1)
      }
    } else {
      // rampUpPart is just a special case of the above formula: rampUpTimeSeconds == seconds
      val rampUpPart = valueAtSecond(rampUpTimeSeconds, rowsPerSecond, rampUpTimeSeconds)
      rampUpPart + (seconds - rampUpTimeSeconds) * rowsPerSecond
    }
  }
}

class RateSourceProvider extends DataSourceV2
  with MicroBatchReadSupport with ContinuousReadSupport with DataSourceRegister {
  import RateSourceProvider._

  private def checkParameters(options: DataSourceOptions): Unit = {
    if (options.get(ROWS_PER_SECOND).isPresent) {
      val rowsPerSecond = options.get(ROWS_PER_SECOND).get().toLong
      if (rowsPerSecond <= 0) {
        throw new IllegalArgumentException(
          s"Invalid value '$rowsPerSecond'. The option 'rowsPerSecond' must be positive")
      }
    }

    if (options.get(RAMP_UP_TIME).isPresent) {
      val rampUpTimeSeconds =
        JavaUtils.timeStringAsSec(options.get(RAMP_UP_TIME).get())
      if (rampUpTimeSeconds < 0) {
        throw new IllegalArgumentException(
          s"Invalid value '$rampUpTimeSeconds'. The option 'rampUpTime' must not be negative")
      }
    }

    if (options.get(NUM_PARTITIONS).isPresent) {
      val numPartitions = options.get(NUM_PARTITIONS).get().toInt
      if (numPartitions <= 0) {
        throw new IllegalArgumentException(
          s"Invalid value '$numPartitions'. The option 'numPartitions' must be positive")
      }
    }
  }

  override def createMicroBatchReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): MicroBatchReader = {
    checkParameters(options)
    if (schema.isPresent) {
      throw new AnalysisException("The rate source does not support a user-specified schema.")
    }

    new RateStreamMicroBatchReader(options, checkpointLocation)
  }

  override def createContinuousReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): ContinuousReader = new RateStreamContinuousReader(options)

  override def shortName(): String = "rate"
}

class RateStreamMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
  extends MicroBatchReader with Logging {
  import RateSourceProvider._

  private[sources] val clock = {
    // The option to use a manual clock is provided only for unit testing purposes.
    if (options.getBoolean("useManualClock", false)) new ManualClock else new SystemClock
  }

  private val rowsPerSecond =
    options.get(ROWS_PER_SECOND).orElse("1").toLong

  private val rampUpTimeSeconds =
    Option(options.get(RAMP_UP_TIME).orElse(null.asInstanceOf[String]))
      .map(JavaUtils.timeStringAsSec(_))
      .getOrElse(0L)

  private val maxSeconds = Long.MaxValue / rowsPerSecond

  if (rampUpTimeSeconds > maxSeconds) {
    throw new ArithmeticException(
      s"Integer overflow. Max offset with $rowsPerSecond rowsPerSecond" +
        s" is $maxSeconds, but 'rampUpTimeSeconds' is $rampUpTimeSeconds.")
  }

  private[sources] val creationTimeMs = {
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    require(session.isDefined)

    val metadataLog =
      new HDFSMetadataLog[LongOffset](session.get, checkpointLocation) {
        override def serialize(metadata: LongOffset, out: OutputStream): Unit = {
          val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
          writer.write("v" + VERSION + "\n")
          writer.write(metadata.json)
          writer.flush
        }

        override def deserialize(in: InputStream): LongOffset = {
          val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
          // HDFSMetadataLog guarantees that it never creates a partial file.
          assert(content.length != 0)
          if (content(0) == 'v') {
            val indexOfNewLine = content.indexOf("\n")
            if (indexOfNewLine > 0) {
              parseVersion(content.substring(0, indexOfNewLine), VERSION)
              LongOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
            } else {
              throw new IllegalStateException(
                s"Log file was malformed: failed to detect the log file version line.")
            }
          } else {
            throw new IllegalStateException(
              s"Log file was malformed: failed to detect the log file version line.")
          }
        }
      }

    metadataLog.get(0).getOrElse {
      val offset = LongOffset(clock.getTimeMillis())
      metadataLog.add(0, offset)
      logInfo(s"Start time: $offset")
      offset
    }.offset
  }

  @volatile private var lastTimeMs: Long = creationTimeMs

  private var start: LongOffset = _
  private var end: LongOffset = _

  override def readSchema(): StructType = SCHEMA

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    this.start = start.orElse(LongOffset(0L)).asInstanceOf[LongOffset]
    this.end = end.orElse {
      val now = clock.getTimeMillis()
      if (lastTimeMs < now) {
        lastTimeMs = now
      }
      LongOffset(TimeUnit.MILLISECONDS.toSeconds(lastTimeMs - creationTimeMs))
    }.asInstanceOf[LongOffset]
  }

  override def getStartOffset(): Offset = {
    if (start == null) throw new IllegalStateException("start offset not set")
    start
  }
  override def getEndOffset(): Offset = {
    if (end == null) throw new IllegalStateException("end offset not set")
    end
  }

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    val startSeconds = LongOffset.convert(start).map(_.offset).getOrElse(0L)
    val endSeconds = LongOffset.convert(end).map(_.offset).getOrElse(0L)
    assert(startSeconds <= endSeconds, s"startSeconds($startSeconds) > endSeconds($endSeconds)")
    if (endSeconds > maxSeconds) {
      throw new ArithmeticException("Integer overflow. Max offset with " +
        s"$rowsPerSecond rowsPerSecond is $maxSeconds, but it's $endSeconds now.")
    }
    // Fix "lastTimeMs" for recovery
    if (lastTimeMs < TimeUnit.SECONDS.toMillis(endSeconds) + creationTimeMs) {
      lastTimeMs = TimeUnit.SECONDS.toMillis(endSeconds) + creationTimeMs
    }
    val rangeStart = valueAtSecond(startSeconds, rowsPerSecond, rampUpTimeSeconds)
    val rangeEnd = valueAtSecond(endSeconds, rowsPerSecond, rampUpTimeSeconds)
    logDebug(s"startSeconds: $startSeconds, endSeconds: $endSeconds, " +
      s"rangeStart: $rangeStart, rangeEnd: $rangeEnd")

    if (rangeStart == rangeEnd) {
      return List.empty.asJava
    }

    val localStartTimeMs = creationTimeMs + TimeUnit.SECONDS.toMillis(startSeconds)
    val relativeMsPerValue =
      TimeUnit.SECONDS.toMillis(endSeconds - startSeconds).toDouble / (rangeEnd - rangeStart)
    val numPartitions = {
      val activeSession = SparkSession.getActiveSession
      require(activeSession.isDefined)
      Option(options.get(NUM_PARTITIONS).orElse(null.asInstanceOf[String]))
        .map(_.toInt)
        .getOrElse(activeSession.get.sparkContext.defaultParallelism)
    }

    (0 until numPartitions).map { p =>
      new DataReaderFactory[Row] {
        override def createDataReader(): DataReader[Row] = {
          new DataReader[Row] {
            var count = 0
            override def next(): Boolean = {
              rangeStart + p + numPartitions * count < rangeEnd
            }

            override def get(): Row = {
              val currValue = rangeStart + p + numPartitions * count
              count += 1
              val relative = math.round((currValue - rangeStart) * relativeMsPerValue)
              Row(
                DateTimeUtils.toJavaTimestamp(
                  DateTimeUtils.fromMillis(relative + localStartTimeMs)),
                currValue
              )
            }

            override def close(): Unit = {}
          }
        }
      }
    }.toList.asJava
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {}

  override def toString: String = s"MicroBatchRateSource[rowsPerSecond=$rowsPerSecond, " +
    s"rampUpTimeSeconds=$rampUpTimeSeconds, " +
    s"numPartitions=${options.get(NUM_PARTITIONS).orElse("default")}"
}

