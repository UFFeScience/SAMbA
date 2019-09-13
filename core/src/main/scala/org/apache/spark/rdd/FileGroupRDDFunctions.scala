package org.apache.spark.rdd

import java.io.File
import java.util.UUID

import br.uff.spark.advancedpipe.{ExecutionPlanning, FileElement, FileGroup}
import org.apache.spark.FutureAction
import org.apache.spark.internal.Logging
import org.jtwig.{JtwigModel, JtwigTemplate}

import scala.collection.JavaConverters._

/**
  * @author Thaylon Guedes Santos
  */
class FileGroupRDDFunctions(self: RDD[FileGroup]) extends Logging with Serializable {

  def runScientificApplication(command: String): RDD[FileGroup] = self.withScope {
    val path = self.conf.get("spark.sciSpark.internalScriptDir") + command
    runCommand { (extraInfo: Map[String, Any], MetaInfo: Seq[FileElement]) =>
      val template = JtwigTemplate.inlineTemplate(path)
      val model = JtwigModel.newModel(extraInfo.asJava.asInstanceOf[java.util.Map[String, AnyRef]])
      val commandArgs = Seq("/bin/bash", "-c", template.render(model))
      if (log.isDebugEnabled)
        logDebug("Command to Run: " + commandArgs.toString())
      val executionPlanning = new ExecutionPlanning(commandArgs)
      executionPlanning
    }
  }

  def runCommand(command: String*): RDD[FileGroup] = self.withScope {
    runCommand(command)
  }

  def runCommand(command: String): RDD[FileGroup] = self.withScope {
    runCommand(PipedRDD.tokenize(command))
  }

  def runCommand(command: String, env: Map[String, String]): RDD[FileGroup] = self.withScope {
    runCommand(PipedRDD.tokenize(command), env)
  }

  def runCommand(
        command: Seq[String],
        env: Map[String, String] = Map(),
        onReadLine: (String) => Unit = null,
        redirectErrorStream: Boolean = false,
        onReadErrorLine: (String) => Unit = null,
        encoding: String = "UTF-8"): RDD[FileGroup] = self.withScope {
    runCommand { (extraInfo: Map[String, Any], fileElements: Seq[FileElement]) =>
      val executionPlanning = new ExecutionPlanning(command, env, redirectErrorStream)
      if (onReadLine != null) {
        executionPlanning.onReadLine = onReadLine
      }
      if (onReadErrorLine != null) {
        executionPlanning.onReadErrorLine = onReadErrorLine
      }
      executionPlanning
    }
  }

  /**
    * Run a Native Command in a FileGroup
    *
    * @param f function that consumer the extra info provided by FileGroup and return
    *          the command which be executed in FileGroup
    */
  def runCommand(f: (Map[String, Any], Seq[FileElement]) => ExecutionPlanning): RDD[FileGroup] = self.withScope {
    val _f = self.context.clean(f)
    new AdvancedPipeRDD(self, _f)
  }

  def saveFilesAt(_file: File): Unit = self.withScope {
    val file = _file.getAbsoluteFile
    self.foreachWithDataElement { t =>
      val id = if (t.id != null) t.id else UUID.randomUUID()
      t.value.saveFilesAt(new File(file, id.toString))
    }
  }

  def saveFilesAtAsync(_file: File): FutureAction[Unit] = self.withScope {
    val file = _file.getAbsoluteFile
    self.foreachAsyncWithDataElement { t =>
      val id = if (t.id != null) t.id else UUID.randomUUID()
      t.value.saveFilesAt(new File(file, id.toString))
    }
  }

}