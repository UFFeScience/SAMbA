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

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.Credentials

import org.apache.spark.SparkConf
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

/**
 * This class loads delegation token providers registered under the YARN-specific
 * [[ServiceCredentialProvider]] interface, as well as the builtin providers defined
 * in [[HadoopDelegationTokenManager]].
 */
private[yarn] class YARNHadoopDelegationTokenManager(
    _sparkConf: SparkConf,
    _hadoopConf: Configuration)
  extends HadoopDelegationTokenManager(_sparkConf, _hadoopConf) {

  private val credentialProviders = {
    ServiceLoader.load(classOf[ServiceCredentialProvider], Utils.getContextOrSparkClassLoader)
      .asScala
      .toList
      .filter { p => isServiceEnabled(p.serviceName) }
      .map { p => (p.serviceName, p) }
      .toMap
  }
  if (credentialProviders.nonEmpty) {
    logDebug("Using the following YARN-specific credential providers: " +
      s"${credentialProviders.keys.mkString(", ")}.")
  }

  override def obtainDelegationTokens(creds: Credentials): Long = {
    val superInterval = super.obtainDelegationTokens(creds)

    credentialProviders.values.flatMap { provider =>
      if (provider.credentialsRequired(hadoopConf)) {
        provider.obtainCredentials(hadoopConf, sparkConf, creds)
      } else {
        logDebug(s"Service ${provider.serviceName} does not require a token." +
          s" Check your configuration to see if security is disabled or not.")
        None
      }
    }.foldLeft(superInterval)(math.min)
  }

  // For testing.
  override def isProviderLoaded(serviceName: String): Boolean = {
    credentialProviders.contains(serviceName) || super.isProviderLoaded(serviceName)
  }

  override protected def fileSystemsToAccess(): Set[FileSystem] = {
    YarnSparkHadoopUtil.hadoopFSsToAccess(sparkConf, hadoopConf)
  }

}
