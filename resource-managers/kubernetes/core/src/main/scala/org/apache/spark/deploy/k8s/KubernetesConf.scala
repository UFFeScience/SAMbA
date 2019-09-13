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
package org.apache.spark.deploy.k8s

import java.util.Locale

import io.fabric8.kubernetes.api.model.{LocalObjectReference, LocalObjectReferenceBuilder, Pod}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit._
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.util.Utils

/**
 * Structure containing metadata for Kubernetes logic to build Spark pods.
 */
private[spark] abstract class KubernetesConf(val sparkConf: SparkConf) {

  val resourceNamePrefix: String
  def labels: Map[String, String]
  def environment: Map[String, String]
  def annotations: Map[String, String]
  def secretEnvNamesToKeyRefs: Map[String, String]
  def secretNamesToMountPaths: Map[String, String]
  def volumes: Seq[KubernetesVolumeSpec]

  def appName: String = get("spark.app.name", "spark")

  def namespace: String = get(KUBERNETES_NAMESPACE)

  def imagePullPolicy: String = get(CONTAINER_IMAGE_PULL_POLICY)

  def imagePullSecrets: Seq[LocalObjectReference] = {
    sparkConf
      .get(IMAGE_PULL_SECRETS)
      .map { secret =>
        new LocalObjectReferenceBuilder().withName(secret).build()
      }
  }

  def nodeSelector: Map[String, String] =
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_NODE_SELECTOR_PREFIX)

  def contains(config: ConfigEntry[_]): Boolean = sparkConf.contains(config)

  def get[T](config: ConfigEntry[T]): T = sparkConf.get(config)

  def get(conf: String): String = sparkConf.get(conf)

  def get(conf: String, defaultValue: String): String = sparkConf.get(conf, defaultValue)

  def getOption(key: String): Option[String] = sparkConf.getOption(key)
}

private[spark] class KubernetesDriverConf(
    sparkConf: SparkConf,
    val appId: String,
    val mainAppResource: MainAppResource,
    val mainClass: String,
    val appArgs: Array[String],
    val pyFiles: Seq[String])
  extends KubernetesConf(sparkConf) {

  override val resourceNamePrefix: String = {
    val custom = if (Utils.isTesting) get(KUBERNETES_DRIVER_POD_NAME_PREFIX) else None
    custom.getOrElse(KubernetesConf.getResourceNamePrefix(appName))
  }

  override def labels: Map[String, String] = {
    val presetLabels = Map(
      SPARK_APP_ID_LABEL -> appId,
      SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)
    val driverCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_LABEL_PREFIX)

    presetLabels.keys.foreach { key =>
      require(
        !driverCustomLabels.contains(key),
        s"Label with key $key is not allowed as it is reserved for Spark bookkeeping operations.")
    }

    driverCustomLabels ++ presetLabels
  }

  override def environment: Map[String, String] = {
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_DRIVER_ENV_PREFIX)
  }

  override def annotations: Map[String, String] = {
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_DRIVER_ANNOTATION_PREFIX)
  }

  override def secretNamesToMountPaths: Map[String, String] = {
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_DRIVER_SECRETS_PREFIX)
  }

  override def secretEnvNamesToKeyRefs: Map[String, String] = {
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_DRIVER_SECRET_KEY_REF_PREFIX)
  }

  override def volumes: Seq[KubernetesVolumeSpec] = {
    KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, KUBERNETES_DRIVER_VOLUMES_PREFIX)
  }
}

private[spark] class KubernetesExecutorConf(
    sparkConf: SparkConf,
    val appId: String,
    val executorId: String,
    val driverPod: Option[Pod])
  extends KubernetesConf(sparkConf) {

  override val resourceNamePrefix: String = {
    get(KUBERNETES_EXECUTOR_POD_NAME_PREFIX).getOrElse(
      KubernetesConf.getResourceNamePrefix(appName))
  }

  override def labels: Map[String, String] = {
    val presetLabels = Map(
      SPARK_EXECUTOR_ID_LABEL -> executorId,
      SPARK_APP_ID_LABEL -> appId,
      SPARK_ROLE_LABEL -> SPARK_POD_EXECUTOR_ROLE)

    val executorCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_EXECUTOR_LABEL_PREFIX)

    presetLabels.keys.foreach { key =>
      require(
        !executorCustomLabels.contains(key),
        s"Custom executor labels cannot contain $key as it is reserved for Spark.")
    }

    executorCustomLabels ++ presetLabels
  }

  override def environment: Map[String, String] = sparkConf.getExecutorEnv.toMap

  override def annotations: Map[String, String] = {
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_EXECUTOR_ANNOTATION_PREFIX)
  }

  override def secretNamesToMountPaths: Map[String, String] = {
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_EXECUTOR_SECRETS_PREFIX)
  }

  override def secretEnvNamesToKeyRefs: Map[String, String] = {
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_EXECUTOR_SECRET_KEY_REF_PREFIX)
  }

  override def volumes: Seq[KubernetesVolumeSpec] = {
    KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, KUBERNETES_EXECUTOR_VOLUMES_PREFIX)
  }

}

private[spark] object KubernetesConf {
  def createDriverConf(
      sparkConf: SparkConf,
      appId: String,
      mainAppResource: MainAppResource,
      mainClass: String,
      appArgs: Array[String],
      maybePyFiles: Option[String]): KubernetesDriverConf = {
    // Parse executor volumes in order to verify configuration before the driver pod is created.
    KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, KUBERNETES_EXECUTOR_VOLUMES_PREFIX)

    val pyFiles = maybePyFiles.map(Utils.stringToSeq).getOrElse(Nil)
    new KubernetesDriverConf(sparkConf.clone(), appId, mainAppResource, mainClass, appArgs,
      pyFiles)
  }

  def createExecutorConf(
      sparkConf: SparkConf,
      executorId: String,
      appId: String,
      driverPod: Option[Pod]): KubernetesExecutorConf = {
    new KubernetesExecutorConf(sparkConf.clone(), appId, executorId, driverPod)
  }

  def getResourceNamePrefix(appName: String): String = {
    val launchTime = System.currentTimeMillis()
    s"$appName-$launchTime"
      .trim
      .toLowerCase(Locale.ROOT)
      .replaceAll("\\s+", "-")
      .replaceAll("\\.", "-")
      .replaceAll("[^a-z0-9\\-]", "")
      .replaceAll("-+", "-")
  }
}
