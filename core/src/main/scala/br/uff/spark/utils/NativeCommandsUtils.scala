package br.uff.spark.utils

import java.io.File

import org.apache.log4j.Logger

import scala.sys.process.Process

/**
  * @author Thaylon Guedes Santos
  */
object NativeCommandsUtils {

  val log: Logger = org.apache.log4j.Logger.getLogger("NativeCommandsUtils")

  def checkExitStatus(exitCode: Int, onSuccess: String, onError: String): Unit = {
    if (exitCode == 0) log.info(onSuccess)
    else log.error(onError)
  }

  def runCommandAndCheckExitStatus(cmd: Seq[String], dir: File, onSuccess: String, onError: String): String = {
    try {
      val result = Process(cmd, dir).!!
      log.info(onSuccess)
      return result
    } catch {
      case e: Exception =>
        log.error(onError)
        throw e
    }
  }

}
