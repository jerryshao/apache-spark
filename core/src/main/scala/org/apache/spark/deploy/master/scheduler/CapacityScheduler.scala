package org.apache.spark.deploy.master.scheduler

import org.apache.spark.SparkConf
import org.apache.spark.deploy.ApplicationDescription
import org.apache.spark.deploy.master.scheduler.CapacityScheduler.RootQueue
import org.apache.spark.deploy.master.{ApplicationInfo, Master}

import scala.collection.mutable

object CapacityScheduler {

  sealed abstract class QueueNode(
      val queueName: String,
      val capacityPredef: Float) {

    lazy val coresPredef: Int = (parent.map(_.coresPredef).get * capacityPredef).toInt

    var parent: Option[QueueNode] = None

    var children: Seq[QueueNode] = Nil

    def usedCores: Int

    def acquireCores(cores: Int): Int

    def releaseCores(cores: Int): Int

    def availableCores = coresPredef - usedCores

    def usedCapacity: Float = usedCores.toFloat / coresPredef

    def queuePath: String = parent.map(p => s"${p.queuePath}/$queueName").getOrElse(s"/$queueName")
  }

  class ParentQueue(
      override val queueName: String,
      override val capacityPredef: Float)
    extends QueueNode(queueName, capacityPredef) {

    def usedCores: Int = {
      var cores: Int = 0
      children.foreach { c => cores += c.usedCores }
      cores
    }

    def acquireCores(cores: Int): Int = {
      var coresNeeded = cores
      var actualCoresAcquired = 0
      for (c <- children; if coresNeeded > 0) {
        val coresGotten = c.acquireCores(coresNeeded)
        actualCoresAcquired += coresGotten
        coresNeeded -= coresGotten
      }

      actualCoresAcquired
    }

    def releaseCores(cores: Int): Int = {
      var coresTobeReleased = cores
      var actualCoresReleased = 0
      for (c <- children; if coresTobeReleased > 0) {
        val coresReleased = c.releaseCores(coresTobeReleased)
        coresTobeReleased -= coresReleased
        actualCoresReleased += coresReleased
      }

      actualCoresReleased
    }
  }

  final class RootQueue extends ParentQueue("root", 1.0F) {
    private var totalCores: Int = _

    def setTotalCores(totalCores: Int): Unit = {
      this.totalCores = totalCores
    }

    override lazy val coresPredef = totalCores
  }

  final class LeafQueue(
      override val queueName: String,
      override val capacityPredef: Float,
      parentNode: QueueNode)
    extends QueueNode(queueName, capacityPredef) {

    private val submittedApps = mutable.HashSet[ApplicationInfo]()

    private var coresUsed: Int = 0

    parent = Some(parentNode)

    override def usedCores: Int = coresUsed

    def requestCoresForApp(app: ApplicationInfo): Int = {
      val cores = acquireCores(app.coresLeft)
      submittedApps += app
      if (cores >= app.coresLeft) {
        cores
      } else {
        cores + requestCoresFromParent(parentNode, app.coresLeft - cores)
      }
    }

    def releaseCoresForApp(app: ApplicationInfo): Unit = {
      submittedApps -= app
      val coresReleased = releaseCores(app.coresGranted)
      if (coresReleased < app.coresGranted) {
        coresReleased + releaseCoresToParent(parentNode, app.coresGranted - coresReleased)
      }
    }

    def acquireCores(cores: Int): Int = {
      val reservedCores = availableCores
      if (reservedCores >= cores) {
        coresUsed += cores
        cores
      } else {
        coresUsed += reservedCores
        reservedCores
      }
    }

    def releaseCores(cores: Int): Int = {
      if (coresUsed - cores < 0) {
        val tmp = coresUsed
        coresUsed = 0
        tmp
      } else {
        coresUsed -= cores
        cores
      }
    }

    private def requestCoresFromParent(parent: QueueNode, cores: Int): Int = {
      val coresAcquired = parent.acquireCores(cores)
      if (coresAcquired >= cores) {
        coresAcquired
      } else {
        coresAcquired + parent.parent.map { p => requestCoresFromParent(p, cores - coresAcquired)}
          .getOrElse(0)
      }
    }

    private def releaseCoresToParent(parent: QueueNode, cores: Int): Int = {
      val coresReleased = parent.releaseCores(cores)
      if (coresReleased >= cores) {
        coresReleased
      } else {
        coresReleased + parent.parent.map(releaseCoresToParent(_, cores - coresReleased))
          .getOrElse(0)
      }
    }
  }


  def main(args: Array[String]): Unit = {
    val rootQueue = new RootQueue(100)
    val queueA = new LeafQueue("QueueA", 0.3F, rootQueue)
    val queueB = new ParentQueue("QueueB", 0.7F)
    val queueC = new LeafQueue("QueueC", 0.4F, queueB)
    val queueD = new LeafQueue("QueueD", 0.6F, queueB)

    rootQueue.children = queueA :: queueB :: Nil

    queueA.parent = Some(rootQueue)

    queueB.parent = Some(rootQueue)
    queueB.children = queueC :: queueD :: Nil

    queueC.parent = Some(queueB)
    queueD.parent = Some(queueB)

    println("-----------queue path------------")
    println(rootQueue.queuePath)
    println(queueB.queuePath)
    println(queueD.queuePath)

    println("-----------queue predefined cores------------")
    println(rootQueue.coresPredef)
    println(queueA.coresPredef)
    println(queueB.coresPredef)
    println(queueC.coresPredef)
    println(queueD.coresPredef)

    val appDesc1 = new ApplicationDescription("aa", Some(40), 1024, null, null)
    val appInfo1 = new ApplicationInfo(0L, "fdsa", appDesc1, null, null, Int.MaxValue)
    val appInfo2 = new ApplicationInfo(0L, "fdsa", appDesc1, null, null, Int.MaxValue)
    val cores = queueC.requestCoresForApp(appInfo1)
    appInfo1.coresGranted = cores
    appInfo2.coresGranted = queueD.requestCoresForApp(appInfo2)
    println("-----------queue used cores after acquired------------")
    println(rootQueue.usedCores)
    println(queueA.usedCores)
    println(queueB.usedCores)
    println(queueC.usedCores)
    println(queueD.usedCores)

    println(rootQueue.usedCapacity)
    println(queueA.usedCapacity)
    println(queueB.usedCapacity)
    println(queueC.usedCapacity)
    println(queueD.usedCapacity)

    println("-----------queue used cores after released------------")
    println(s"${queueC.releaseCoresForApp(appInfo1)}")
    println(rootQueue.usedCores)
    println(queueA.usedCores)
    println(queueB.usedCores)
    println(queueC.usedCores)
    println(queueD.usedCores)

    println(rootQueue.usedCapacity)
    println(queueA.usedCapacity)
    println(queueB.usedCapacity)
    println(queueC.usedCapacity)
    println(queueD.usedCapacity)
  }
}

private[spark] class CapacityScheduler(val master: Master) extends ResourceScheduler {

  private lazy val totalCores = master.workers
  private var rootQueue: RootQueue =

  override def initialize(conf: SparkConf): Unit = {
    conf.get("spark.deploy.")

  }

  override def schedule(): Unit = ???

}
