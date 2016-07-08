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

package org.apache.spark.deploy.yarn.security

import java.io.{ByteArrayInputStream, DataInputStream}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.Credentials

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

private[yarn] class HDFSCredentialProvider(sparkConf: SparkConf)
    extends ServiceCredentialProvider with Logging {
  // NameNode to access, used to get tokens from different FileSystems
  private val nnsToAccess = {
    val tmpConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val stageDir = sparkConf.get(STAGING_DIR).map(new Path(_))
      .getOrElse(FileSystem.get(tmpConf).getHomeDirectory)
    sparkConf.get(NAMENODES_TO_ACCESS)
      .map(new Path(_)) ++ Seq(stageDir)
      .toSet
  }

  // Token renewal interval, this value will be set in the first call,
  // if None means no token renewer specified, so cannot get token renewal interval.
  private var tokenRenewalInterval: Option[Long] = null

  override val  serviceName: String = "hdfs"

  override def obtainCredentials(hadoopConf: Configuration): (Credentials, Option[Long]) = {
    // Obtain tokens from HDFS and add into Credentials
    val tmpCreds = new Credentials()
    nnsToAccess.foreach { dst =>
      val dstFs = dst.getFileSystem(hadoopConf)
      logInfo("getting token for namenode: " + dst)
      dstFs.addDelegationTokens(getTokenRenewer(hadoopConf), tmpCreds)
    }

    // Get the token renewal interval if it is not set. It will only be called once.
    if (tokenRenewalInterval == null) {
      getTokenRenewalInterval(hadoopConf)
    }

    // Get the time length from now to next renewal.
    val timeToNextRenewal = tokenRenewalInterval.map { interval =>
      tmpCreds.getAllTokens.asScala
        .filter(_.getKind == DelegationTokenIdentifier.HDFS_DELEGATION_KIND)
        .map { t =>
          val identifier = new DelegationTokenIdentifier()
          identifier.readFields(new DataInputStream(new ByteArrayInputStream(t.getIdentifier)))
          identifier.getIssueDate + interval - System.currentTimeMillis()
      }.foldLeft(0L)(math.max)
    }

    (tmpCreds, timeToNextRenewal)
  }

  private def getTokenRenewalInterval(hadoopConf: Configuration): Unit = {
    // We cannot use the tokens generated with renewer yarn. Trying to renew
    // those will fail with an access control issue. So create new tokens with the logged in
    // user as renewer.
    tokenRenewalInterval = sparkConf.get(PRINCIPAL).map { renewer =>
      val creds = new Credentials()
      nnsToAccess.foreach { dst =>
        val dstFs = dst.getFileSystem(hadoopConf)
        dstFs.addDelegationTokens(renewer, creds)
      }
      val t = creds.getAllTokens.asScala
        .filter(_.getKind == DelegationTokenIdentifier.HDFS_DELEGATION_KIND)
        .head
      val newExpiration = t.renew(hadoopConf)
      val identifier = new DelegationTokenIdentifier()
      identifier.readFields(new DataInputStream(new ByteArrayInputStream(t.getIdentifier)))
      val interval = newExpiration - identifier.getIssueDate
      logInfo(s"Renewal Interval set to $interval")
      interval
    }
  }

  private def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    logDebug("delegation token renewer is: " + delegTokenRenewer)
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer"
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }

    delegTokenRenewer
  }
}
