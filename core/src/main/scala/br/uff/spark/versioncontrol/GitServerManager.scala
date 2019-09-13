package br.uff.spark.versioncontrol

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.log4j.Logger

import scala.sys.process.Process
import br.uff.spark.utils.NativeCommandsUtils._

import scala.sys.process._

/**
  * @author Thaylon Guedes Santos
  *
  *         All these methods are executed only in the master node
  */
class GitServerManager {

  private val log: Logger = org.apache.log4j.Logger.getLogger(classOf[GitServerManager])
  private var gitblitServerProcess: Process = null
  //1 - Not Started
  //2 - Started
  //3 - Finished
  private val serverStatus = new AtomicInteger(1)

  /**
    * Start the log server
    *
    * @param logBaseName
    */
  def startGitblitServer(logBaseName: String): Unit = {
    log.info("Starting the GitBlit Server")

    var SPARK_HOME = System.getenv("SPARK_HOME")
    if (SPARK_HOME == null)
      SPARK_HOME = new File("").getAbsolutePath
    val gitBlitDir = new File(new File(SPARK_HOME), s"gitblit").getAbsoluteFile
    val logDir = new File(gitBlitDir, "logs")
    if (!logDir.exists())
      logDir.mkdir()
    val logFile = s"${logBaseName}_${System.currentTimeMillis()}.txt"
    val cmd = Seq("/bin/bash", "-c", s"sh gitblit.sh &> logs/'$logFile'")

    new Thread("GitBlit Server") {
      override def run(): Unit = {
        gitblitServerProcess = Process(cmd, gitBlitDir).run()
        serverStatus.set(2)
        val exitValue = gitblitServerProcess.exitValue()
        serverStatus.set(3)
      }
    }.start()

    log.info("Waiting 10 seconds for the Git server init")
    Thread.sleep(10000)

    if (serverStatus.get() == 2) {
      log.info("The git server was started with success at: https://{masternode}:8443/")

      //Starting git merge service
      new GitMergeBranchService().start()

    } else {
      log.error(s"There are a problem with git server, please, check this file: ${logDir.getAbsolutePath}/${logFile}")
    }
  }

  /**
    * Function that return the full path to specific repository inside of git server
    *
    * @param projectName
    * @return File with full path
    */
  def getGitRepositoryDir(projectName: String): File = {
    var SPARK_HOME = System.getenv("SPARK_HOME")
    if (SPARK_HOME == null)
      SPARK_HOME = new File("").getAbsolutePath
    val file = new File(new File(SPARK_HOME), s"gitblit/data/git/${projectName}.git").getAbsoluteFile
    return file
  }

  /**
    * Create a new branch
    *
    * @param projectName - The git repository
    * @param executionID - The git branch name
    * @return
    */
  def createNewExecutionBranch(projectName: String, executionID: UUID): File = {
    val file = getGitRepositoryDir(projectName)
    val exitCode = Process(Seq("git", "branch", executionID.toString), file).!
    checkExitStatus(exitCode, "Success created the execution branch", "Failed to create the execution branch")
    file
  }

  /**
    * Create, if not exists, a repository for the project name passed in args
    *
    * @param projectName
    */
  def createNewRepository(projectName: String): Unit = {
    val repositoryDir = getGitRepositoryDir(projectName)
    log.info("Creating the repository at: " + repositoryDir)
    if (!repositoryDir.exists) {
      repositoryDir.mkdirs()
      //Initiating the repository
      var exitCode = Seq("git", "init", repositoryDir.getAbsolutePath, "--bare").!
      checkExitStatus(exitCode, "Success created repository", "Failed to create repository")
      //Creating the root tree element
      val hashCodeOfTree = runCommandAndCheckExitStatus(
        Seq("git", "write-tree"), repositoryDir,
        "The command write-tree was executed with success",
        "Fail on run the write-tree command"
      ).replace("\n", "")
      //Committing that tree
      val commitHashID = runCommandAndCheckExitStatus(
        Seq("git", "commit-tree", "-m", "Zero Point", hashCodeOfTree), repositoryDir,
        s"The tree $hashCodeOfTree was committed with success.",
        s"Fail on try to commit the tree $hashCodeOfTree"
      ).replaceAllLiterally("\n", "")
      //Putting that commit as the commit point of master branch
      exitCode = Process(Seq("git", "update-ref", "refs/heads/master", commitHashID), repositoryDir).!
      checkExitStatus(exitCode,
        "Success on update the master header to the zero point commit",
        s"Failed to updates the master header to commit $commitHashID")
    }
  }

}
