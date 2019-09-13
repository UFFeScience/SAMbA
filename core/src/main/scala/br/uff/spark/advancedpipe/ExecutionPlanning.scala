package br.uff.spark.advancedpipe

import org.apache.spark.internal.Logging

/**
  * @author Thaylon Guedes Santos
  */
class ExecutionPlanning(
    val command: Seq[String],
    val env: Map[String, String] = Map(),
    val redirectErrorStream: Boolean = false,
    val encoding: String = "UTF-8")
  extends Logging with Serializable {

  var onReadLine: (String) => Unit = line => logInfo(line)
  var onReadErrorLine: (String) => Unit = errorLine => logError("Error stream line: " + errorLine)

  var filterFilesForGeneratedRDD: (FileElement) => Boolean = (_) => true
  var getExtrasInfoForGeneratedRDD: () => Map[String, AnyRef] = null

}
