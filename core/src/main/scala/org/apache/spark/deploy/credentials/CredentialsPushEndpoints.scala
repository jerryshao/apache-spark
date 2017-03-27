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

package org.apache.spark.deploy.credentials

import java.io.{ByteArrayInputStream, DataInputStream}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.control.NonFatal

import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.{SecurityManager, SparkConf, SparkContext, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateCredentials
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A [[CredentialsPushEndpoint]] receives [[UpdateCredentials]] from remote endpoint and updates
 * credentials in Driver side UGI, also propagates this credentials to all the remote Executors
 * and AM (in yarn mode). The whole RPC propagation is chained by Scala Future, user could wait
 * on the Future until all the updates after finished successfully.
 */
private[spark] class CredentialsPushEndpoint(override val rpcEnv: RpcEnv, sc: SparkContext)
    extends RpcEndpoint with Logging {
  private implicit val executor = ThreadUtils.sameThread

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case UpdateCredentials(c) =>
        sc.schedulerBackend match {
          case s: CoarseGrainedSchedulerBackend =>
            logInfo(s"Update credentials in driver")
            try {
              val dataInput = new DataInputStream(new ByteArrayInputStream(c))
              val credentials = new Credentials
              credentials.readFields(dataInput)
              logInfo("Update credentials with Tokens " +
                s"${credentials.getAllTokens.asScala.map(_.getKind.toString).mkString(",")} " +
                "in driver")
              UserGroupInformation.getCurrentUser.addCredentials(credentials)
            } catch {
              case NonFatal(e) => logWarning(s"Failed to update credentials", e)
            }

            val future = s.updateCredentials(c)
            future onSuccess {
              case b => context.reply(b)
            }

            future onFailure {
              case NonFatal(e) => context.sendFailure(e)
            }

          case _ =>
            throw new SparkException("Update credentials on" +
              s" ${sc.schedulerBackend.getClass.getSimpleName} is not supported")
        }
  }
}

private[spark] object CredentialsPushEndpoint {
  val ENDPOINT_NAME = "CREDENTIALS_ENDPOINT"
}

/**
 * A [[CredentialsPushEndpointRef]] used to connect to [[CredentialsPushEndpoint]] and push
 * credentials to the endpoint. Each cluster manager should implement its own way to provide
 * driver's RPC host and port to connect to.
 */
private[deploy] abstract class CredentialsPushEndpointRef(
    sparkConf: SparkConf,
    applicationId: String) {

  def driverHost: String
  def driverPort: Int

  private val hadoopConf = SparkHadoopUtil.get.conf
  private val securityManager = new SecurityManager(sparkConf)
  private lazy val credentialMgr = new ConfigurableCredentialManager(sparkConf, hadoopConf)

  private val rpcEnv = RpcEnv.create(
      "sparkLauncher", Utils.localHostName(), 0, sparkConf, securityManager, clientMode = true)

  private def endpointRef(): RpcEndpointRef = {
    rpcEnv.setupEndpointRef(RpcAddress(driverHost, driverPort),
      CredentialsPushEndpoint.ENDPOINT_NAME)
  }

  def updateCredentials(applicationId: String): Future[Boolean] = {
    val credentials = new Credentials()
    val timeOfNextUpdate = credentialMgr.obtainCredentials(hadoopConf, credentials)
    updateCredentials(applicationId, credentials)
  }

  def updateCredentials(applicationId: String, credentials: Credentials): Future[Boolean] = {
    val dob = new DataOutputBuffer()
    credentials.write(dob)
    endpointRef().ask[Boolean](UpdateCredentials(dob.getData))
  }
}

object CredentialsPushEndpointRef {
  def createEndpointRef(applicationId: String, sparkConf: SparkConf): CredentialsPushEndpointRef = {
    if (sparkConf.get("spark.master").startsWith("yarn")) {
      try {
        Utils.classForName("org.apache.spark.deploy.yarn.YarnCredentialsUpdateEndpointRef")
          .getConstructor(classOf[String], classOf[SparkConf])
          .newInstance(applicationId, sparkConf)
          .asInstanceOf[CredentialsPushEndpointRef]
      } catch {
        case NonFatal(e) =>
          throw new SparkException("Failed to create CredentialsUpdateEndpointRef", e)
      }
    } else {
      throw new SparkException(s"Credentials update is not supported for " +
        s"${sparkConf.get("spark.master")} cluster manager")
    }
  }
}
