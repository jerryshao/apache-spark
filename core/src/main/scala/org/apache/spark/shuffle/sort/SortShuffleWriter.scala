package org.apache.spark.shuffle.sort

import java.io.File
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.executor.ShuffleWriteMetrics

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.util.TimeStampedHashMap
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}
import org.apache.spark.storage.{ShuffleBlockId, FileSegment}

class SortShuffleWriter[K, V](
    handle: BaseShuffleHandle[K, V, Any],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {
  import SortShuffleWriter._

  private val dep = handle.dependency
  private val numOutputSplits = dep.partitioner.numPartitions

  private val blockManager = SparkEnv.get.blockManager
  private val conf = blockManager.conf
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
  private val bufSize = conf.getInt("spark.shuffle.file.buffer.kb", 100) * 1024

  private val partKeyComparator = new Comparator[(K, _)] {
    // Use hashcode comparison temporarily, consider keyOrdering
    val keyComparator = new ExternalAppendOnlyMap.KCComparator[K, Any]

    def compare(comp1: (K, _), comp2: (K, _)): Int = {
      val hash1 = dep.partitioner.getPartition(comp1._1)
      val hash2 = dep.partitioner.getPartition(comp2._1)
      if (hash1 != hash2) {
        hash1 - hash2
      } else {
        if (dep.aggregator.isDefined) {
          keyComparator.compare(comp1, comp2)
        } else {
          0
        }
      }
    }
  }

  private val aggregator = dep.aggregator.getOrElse {
    Aggregator[K, V, ArrayBuffer[V]](
        v => ArrayBuffer(v),
        (c, v) => c += v,
        (c1, c2) => c1 ++= c2)
      .asInstanceOf[Aggregator[K, V, Any]]
  }
  aggregator.setComparator(partKeyComparator)

  shuffleMetadataMap.putIfAbsent(dep.shuffleId, ShuffleMetadata(dep.shuffleId, numOutputSplits))

  private val shuffleMetadata = shuffleMetadataMap(dep.shuffleId)
  private var mapStatus: Option[MapStatus] = None

  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    val iter = aggregator.combineValuesByKey(records, context)

    val shuffleBlockId = ShuffleBlockId(dep.shuffleId, mapId, nextFileId.getAndIncrement())
    val shuffleFile = blockManager.diskBlockManager.getFile(shuffleBlockId)
    var writer = blockManager.getDiskWriter(shuffleBlockId, shuffleFile, ser, bufSize)

    val offsets = new Array[Long](numOutputSplits)
    val compressedSizes = new Array[Byte](numOutputSplits)
    var previousBucketId: Int = -1
    var totalBytes = 0L
    var totalTime = 0L

    try {
      for (it <- iter) {
        val bucketId = dep.partitioner.getPartition(it._1)
        if (previousBucketId == -1) {
          previousBucketId = bucketId
        } else if (previousBucketId != bucketId) {
          writer.commit()
          writer.close()
          val fileSegment = writer.fileSegment()
          offsets(previousBucketId) = fileSegment.offset
          totalBytes += fileSegment.length
          totalTime += writer.timeWriting()
          previousBucketId = bucketId

          // Reopen the file for another partition
          writer = blockManager.getDiskWriter(shuffleBlockId, shuffleFile, ser, bufSize)
        } else {
        }

        it._2 match {
          case p: ArrayBuffer[_] => p.foreach(r => writer.write((it._1, r)))
          case _ => writer.write(it)
        }
      }

      writer.commit()
    } catch {
      case e: Exception =>
        writer.revertPartialWrites()
        throw e
    } finally {
      writer.close()
    }

    val fileSegment = writer.fileSegment()
    offsets(previousBucketId) = fileSegment.offset
    totalBytes += fileSegment.length
    totalTime += writer.timeWriting()

    val shuffleMetrics = new ShuffleWriteMetrics
    shuffleMetrics.shuffleBytesWritten = totalBytes
    shuffleMetrics.shuffleWriteTime = totalTime
    context.taskMetrics.shuffleWriteMetrics = Some(shuffleMetrics)

    var i = 1
    while (i < offsets.length) {
      if (offsets(i) == 0) {
        offsets(i) = offsets(i - 1)
      }
      compressedSizes(i - 1) = MapOutputTracker.compressSize(offsets(i) - offsets(i - 1))
      i += 1
    }
    compressedSizes(i - 1) = MapOutputTracker.compressSize(shuffleFile.length() - offsets(i - 1))

    shuffleMetadata.recordMapOutput(mapId, shuffleFile, offsets)
    mapStatus = Some(new MapStatus(blockManager.blockManagerId, compressedSizes))
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    mapStatus
  }
}

object SortShuffleWriter {

  // missing clean shuffle data.
  private val shuffleMetadataMap = new TimeStampedHashMap[Int, ShuffleMetadata]
  private val nextFileId = new AtomicInteger(0)

  def getBlockLocation(id: ShuffleBlockId): FileSegment = {
    val shuffleMetadata = shuffleMetadataMap(id.shuffleId)
    shuffleMetadata.getFileSegment(id.mapId, id.reduceId).getOrElse(
      throw new IllegalStateException(s"Failed to find shuffle block: $id"))
  }

  def removeShuffle(shuffleId: Int) {
    val cleaned = shuffleMetadataMap(shuffleId)
    cleaned.cleanup()
    shuffleMetadataMap.remove(shuffleId)
  }

  case class ShuffleMetadata(val shuffleId: Int, val numOutputSplits: Int) {
    // To keep each map output's file and offsets
    private val offsetsByReducer = new ConcurrentHashMap[Int, (File, Array[Long])]()

    def recordMapOutput(mapId: Int, shuffleFile: File, offsets: Array[Long]) {
      offsetsByReducer.put(mapId, (shuffleFile, offsets))
    }

    def getFileSegment(mapId: Int, reduceId: Int): Option[FileSegment] = {
      val (file, offsets) = offsetsByReducer.get(mapId)
      if (offsets != null) {
        val offset = offsets(reduceId)
        val length = if (reduceId + 1 < numOutputSplits) {
          offsets(reduceId + 1) - offset
        } else {
          file.length() - offset
        }

        assert(length >= 0)
        Some(new FileSegment(file, offset, length))
      } else {
        None
      }
    }

    def cleanup() {
      import scala.collection.JavaConversions._
      offsetsByReducer.foreach { case (_, (f, _)) => f.delete() }
    }
  }
}
