package br.uff.spark.versioncontrol

import java.io.File
import java.net.{ServerSocket, Socket}
import java.text.MessageFormat
import java.util.concurrent.Executors

import org.apache.log4j.Logger
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.RefUpdate.Result
import org.eclipse.jgit.lib._
import org.eclipse.jgit.merge.{MergeStrategy, RecursiveMerger}
import org.eclipse.jgit.revwalk.RevWalk

/**
  * This service will handle the git post-update hook event. The hook will notify this service when a
  * machine-executor make a push with new data, then this service will make the merge of the machine
  * branch into your parent branch.
  *
  * @author Thaylon Guedes Santos
  */

class GitMergeBranchService extends Thread {

  var log: Logger = org.apache.log4j.Logger.getLogger(classOf[GitMergeBranchService])
  val LISTEN_PORT = 5050

  private val executorService = Executors.newSingleThreadExecutor()

  override def run(): Unit = {
    val serverSocket = new ServerSocket(LISTEN_PORT)
    while (true) {
      val socket = serverSocket.accept()
      processRequest(socket)
    }
  }

  def processRequest(socket: Socket): Unit = {
    val msg = scala.io.Source.fromInputStream(socket.getInputStream).mkString
    socket.close()
    executorService.submit(new Runnable {
      override def run(): Unit = {
        processMerge(msg)
      }
    })
  }

  def processMerge(msg: String): Unit = {

    val data = msg.split(";")
    val repositoryDir = data(0)
    val srcBranch = data(1)
    val toBranch = "refs/heads/" + data(2)
    val machineID = data(3)
    val repository = Git.open(new File(repositoryDir)).getRepository
    try {
      val revWalk = new RevWalk(repository)
      val branchTip = revWalk.lookupCommit(repository.resolve(toBranch))
      val srcTip = revWalk.lookupCommit(repository.resolve(srcBranch))
      if (!revWalk.isMergedInto(srcTip, branchTip)) {
        val merger = MergeStrategy.RECURSIVE.newMerger(repository, true).asInstanceOf[RecursiveMerger]
        val merged = merger.merge(branchTip, srcTip)
        if (!merged)
          log.error(s"The branch msg ${msg} can't be merged")

        val treeId = merger.getResultTreeId
        val odi = repository.newObjectInserter

        try {
          val committer = new PersonIdent("Machine " + machineID, "machine_${machineID}@fakemail.org")
          val commitBuilder = new CommitBuilder
          commitBuilder.setCommitter(committer)
          commitBuilder.setAuthor(committer)
          commitBuilder.setEncoding(Constants.CHARSET)
          val message = MessageFormat.format("merge {0} into {1}", srcTip.getName, branchTip.getName)
          commitBuilder.setMessage(message)
          commitBuilder.setParentIds(Seq(branchTip.getId, srcTip.getId): _*)
          commitBuilder.setTreeId(treeId)
          val mergeCommitId = odi.insert(commitBuilder)
          odi.flush()
          val mergeCommit = revWalk.parseCommit(mergeCommitId)
          val mergeRefUpdate = repository.updateRef(toBranch)
          mergeRefUpdate.setNewObjectId(mergeCommitId)
          mergeRefUpdate.setRefLogMessage("commit: " + mergeCommit.getShortMessage, false)
          val rc = mergeRefUpdate.update
          if (rc == Result.FAST_FORWARD) {
            log.info("The repository was merged with success")
          } else {
            log.error(MessageFormat.format("Unexpected result \"{0}\" when merging commit {1} into {2} in {3}", rc.name, srcTip.getName, branchTip.getName, repository.getDirectory))
          }
        } finally odi.close()

      } else {
        log.info(s"This branch ${msg} already merged")
      }
    } catch {
      case e: Exception => {
        log.error("Error", e)
      }
    }
  }

}
