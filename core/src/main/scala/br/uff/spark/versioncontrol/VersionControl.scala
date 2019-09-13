package br.uff.spark.versioncontrol

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import br.uff.spark.{DataElement, DataflowProvenance, Task}
import br.uff.spark.advancedpipe.FileGroup
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import br.uff.spark.utils.NativeCommandsUtils._
import org.apache.spark.util.ShutdownHookManagerBridge

import scala.sys.process._

/**
  * @author Thaylon Guedes Santos
  */
object VersionControl {

  private val instance = new VersionControl

  def getInstance: VersionControl = instance

  var isEnable = true

  def checkIsEnable(conf: SparkConf): Unit = {
    isEnable = conf.get("spark.sciSpark.versionControl").toBoolean
  }

}

class VersionControl private() {

  private val log: Logger = org.apache.log4j.Logger.getLogger(classOf[VersionControl])
  private var tempDir: File = null
  private var executorService = Executors.newSingleThreadExecutor()
  private var somethingWasWriting = false
  private var isRemoteRepository = false
  private val gitServerManager = new GitServerManager
  private var currentBranchName: String = null
  private var executorID = "Master"

  def startGitServer(baseNameLog: String): Unit = {
    gitServerManager.startGitblitServer(baseNameLog)
  }

  def cloneSameMachineRepository(sparkContext: SparkContext): Unit = {
    cloneRepository(sparkContext.executionID, sparkContext.appName, sparkContext.getConf.get("spark.master"), null, "master")
  }

  def cloneRepository(executionID: UUID, appName: String, masterProp: String, masterHostName: String, executorID: String): Unit = {
    isRemoteRepository = !masterProp.startsWith("local")
    currentBranchName = executionID.toString
    this.executorID = executorID.toString
    var exitValue = if (isRemoteRepository) {
      if (masterHostName == null) throw new NullPointerException("The Master Host Name  is null")
      tempDir = new File(s"/tmp/git-${executionID}_${UUID.randomUUID()}")
      Seq("git", "clone", "--single-branch", "-c", "http.sslVerify=false", "-b", executionID.toString, s"https://admin:admin@${masterHostName}:8443/r/${appName}.git", tempDir.getAbsolutePath).!
    } else {
      tempDir = new File("/tmp/git-" + executionID)
      val repository = gitServerManager.getGitRepositoryDir(appName)
      Seq("git", "clone", "--single-branch", "-b", executionID.toString, repository.getAbsolutePath, tempDir.getAbsolutePath, "--no-hardlinks").!
    }
    checkExitStatus(exitValue, "Success clone the repository", "Failed to clone the repository")
    if (exitValue == 0 && isRemoteRepository) {
      currentBranchName += s"_machine_id=${executorID}"
      exitValue = Process(Seq("git", "checkout", "-b", currentBranchName), tempDir).!
      checkExitStatus(exitValue, s"Success created the new branch in the repository: $currentBranchName", s"Failed to create the new branch: $currentBranchName")
    }
  }

  def initAll(sparkContext: SparkContext): Unit = {
    gitServerManager.createNewRepository(sparkContext.appName)
    gitServerManager.createNewExecutionBranch(sparkContext.appName, sparkContext.executionID)
    if (sparkContext.getConf.get("spark.master").startsWith("local"))
      cloneSameMachineRepository(sparkContext)
    if (executorService.isShutdown) {
      executorService = Executors.newSingleThreadExecutor()
    }
  }

  def writeFileGroup(task: Task, fileGroup: FileGroup, dataElementID: UUID): Unit = {
    if (fileGroup.getName == null) {
      log.error("This file group doesn't have the git id")
      return
    }

    if (fileGroup.getFileElements.isEmpty) {
      log.error("The list of file can't be empty")
      return
    }

    executorService.submit(new Runnable {
      override def run(): Unit = {
        somethingWasWriting = true
        val folderInRepository = task.description + "/" + fileGroup.getName
        val target = new File(tempDir, folderInRepository)
        DataflowProvenance.getInstance.insertFileGroupReference(dataElementID, folderInRepository)
        if (!target.exists()) {
          target.mkdirs()
        }
        for (fileElement <- fileGroup.getFileElements) {
          val file = Paths.get(target.getAbsolutePath, fileElement.getFilePath, fileElement.getFileName)
          if (Files.exists(file)) {
            Files.delete(file)
          }
          Files.createFile(file)
          FileUtils.copyInputStreamToFile(fileElement.getContents.toInputStream, file.toFile)
        }
        var exitValue = Process(Seq("git", "add", "-A"), tempDir).!
        checkExitStatus(exitValue, "Success add all files to commit", "Failed on add all files to commit")
        exitValue = Process(
          Seq(
            "git", "commit",
            "-m", s"Task id: ${task.id} description: ${task.description}. Committing File Group: ${fileGroup.getName}"
          ),
          tempDir,
          "GIT_COMMITTER_NAME" -> s"Machine ${executorID}",
          "GIT_AUTHOR_NAME" -> s"Machine ${executorID}",
          "GIT_COMMITTER_EMAIL" -> s"machine_${executorID}@fakemail.org",
          "GIT_AUTHOR_EMAIL" -> s"machine_${executorID}@fakemail.org"
        ).!
        checkExitStatus(exitValue, "Success commit the new files", "Failed on commit the new files")
      }
    })
  }

  /**
    * Commit all changes and push them to the master repository
    */
  def finish(): Unit = {
    if (VersionControl.isEnable) {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          if (somethingWasWriting) {
            val exitValue = if (isRemoteRepository)
              Process(Seq("git", "push", "--set-upstream", "origin", currentBranchName), tempDir).!
            else
              Process(Seq("git", "push"), tempDir).!
            checkExitStatus(exitValue, "Success push the new files", "Failed on push the new files")
            if (exitValue != 0) {
              log.error("There is a problem with \"git\" push command.")
              return
            }
          }
          if (tempDir != null) {
            ShutdownHookManagerBridge.registerShutdownDeleteDir(tempDir)
          }
        }
      })
    }
    executorService.shutdown()
    executorService.awaitTermination(2, TimeUnit.HOURS) // TODO
  }

}