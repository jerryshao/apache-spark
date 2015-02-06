package org.apache.spark.deploy.master.scheduler

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master._

private[spark] final class FIFOScheduler(val master: Master) extends ResourceScheduler {

  override def initialize(conf: SparkConf): Unit = { }

  override def schedule(): Unit = {
    if (master.state != RecoveryState.ALIVE) { return }

    // First schedule drivers, they take strict precedence over applications
    // Randomization helps balance drivers
    val shuffledAliveWorkers = Random.shuffle(
      master.workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0

    for (driver <- master.waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      var launched = false
      var numWorkersVisited = 0
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          master.launchDriver(worker, driver)
          master.waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
    }

    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    if (master.spreadOutApps) {
      // Try to spread out each app among all the nodes, until it has all its cores
      for (app <- master.waitingApps if app.coresLeft > 0) {
        val usableWorkers = master.workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(canUse(app, _)).sortBy(_.coresFree).reverse
        val numUsable = usableWorkers.length
        val assigned = new Array[Int](numUsable) // Number of cores to give on each node
        var toAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
        var pos = 0
        while (toAssign > 0) {
          if (usableWorkers(pos).coresFree - assigned(pos) > 0) {
            toAssign -= 1
            assigned(pos) += 1
          }
          pos = (pos + 1) % numUsable
        }
        // Now that we've decided how many cores to give on each node, let's actually give them
        for (pos <- 0 until numUsable) {
          if (assigned(pos) > 0) {
            val exec = app.addExecutor(usableWorkers(pos), assigned(pos))
            master.launchExecutor(usableWorkers(pos), exec)
            app.state = ApplicationState.RUNNING
          }
        }
      }
    } else {
      // Pack each app into as few nodes as possible until we've assigned all its cores
      for (worker <- master.workers if worker.coresFree > 0 && worker.state == WorkerState.ALIVE) {
        for (app <- master.waitingApps if app.coresLeft > 0) {
          if (canUse(app, worker)) {
            val coresToUse = math.min(worker.coresFree, app.coresLeft)
            if (coresToUse > 0) {
              val exec = app.addExecutor(worker, coresToUse)
              master.launchExecutor(worker, exec)
              app.state = ApplicationState.RUNNING
            }
          }
        }
      }
    }
  }

  /**
   * Can an app use the given worker? True if the worker has enough memory and we haven't already
   * launched an executor for the app on it (right now the standalone backend doesn't like having
   * two executors on the same worker).
   */
  private def canUse(app: ApplicationInfo, worker: WorkerInfo): Boolean = {
    worker.memoryFree >= app.desc.memoryPerSlave && !worker.hasExecutor(app)
  }


}
