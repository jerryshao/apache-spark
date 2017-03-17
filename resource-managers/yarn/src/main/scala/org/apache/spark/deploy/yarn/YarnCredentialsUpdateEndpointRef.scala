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

package org.apache.spark.deploy.yarn

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

import org.apache.spark.SparkException
import org.apache.spark.deploy.credentials.CredentialsUpdateEndpointRef
import org.apache.spark.rpc.RpcEnv

class YarnCredentialsUpdateEndpointRef(applicationId: String, rpcEnv: RpcEnv)
  extends CredentialsUpdateEndpointRef(applicationId, rpcEnv) {

  private lazy val yarnClient = {
    val yarnConf = new YarnConfiguration(new Configuration())
    val c = YarnClient.createYarnClient()
    c.init(yarnConf)
    c.start()
    c
  }

  val hostPort = Try {
    val report = yarnClient.getApplicationReport(ConverterUtils.toApplicationId(applicationId))
    if (report.getYarnApplicationState != YarnApplicationState.RUNNING) {
      throw new IllegalStateException(s"Application $applicationId is not running")
    }

    if (report.getHost == "N/A" || report.getRpcPort == -1) {
      throw new IllegalStateException(s"Cannot get RPC host port of application $applicationId " +
        "from RM")
    }

    (report.getHost, report.getRpcPort)
  }

  override def driverHost: String = {
    hostPort match {
      case Success(h) => h._1
      case Failure(e) =>
        throw new SparkException(s"Failed to get host from application $applicationId", e)
    }
  }

  override def driverPort: Int = {
    hostPort match {
      case Success(h) => h._2
      case Failure(e) =>
        throw new SparkException(s"Failed to get port from application $applicationId", e)
    }
  }
}
