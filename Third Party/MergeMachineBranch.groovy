/*
 * Copyright 2011 gitblit.com.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.gitblit.utils.JGitUtils
import org.eclipse.jgit.lib.PersonIdent
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.transport.ReceiveCommand
import org.eclipse.jgit.transport.ReceiveCommand.Result

/**
 * @autor Thaylon Guedes Santos
 * @email thaylongs@gmail.com
 */

// Indicate we have started the script
logger.info("Merge Machine Branch hook triggered by ${user.username} for ${repository.name}")

/**
 * In the SciSpark, when it is running in cluster mode, is created one branch for each slave which
 * has the same parent commit, the execution id branch.  This hook serve to merge all branch of
 * each slave in the execution branch.
 * */

/**
 * Return the parent branch
 * @param refName
 * @return
 */
String getBranchParentName(String refName) {
    def temp = refName.substring("refs/heads/".size())
    temp = temp.substring(0, temp.indexOf("_machine"))
    return temp
}

void sendData(String data) {
    try {
        def socket = new Socket("localhost", 5050)
        def printWriter = new PrintWriter(socket.getOutputStream())
        printWriter.println(data)
        printWriter.flush()
        printWriter.close()
        socket.close()
        logger.info("The msg: ${data} was sended with success!!")
    } catch (Exception e) {
        logger.error("Error on try send the commit msg to service.", e)
    }

}

Repository repository = gitblit.getRepository(repository.name)

for (ReceiveCommand command : commands) {
    if (command.getResult() == Result.OK) {
        def branchRf = command.refName
        if (branchRf.contains("_machine_id")) {
            def parentBranch = getBranchParentName(branchRf)
            def machineID = branchRf.split("=")[1]
            def msg = "${repository.getDirectory()};${branchRf};${parentBranch};${machineID}"
            sendData(msg)
        }
    }
}


// close the repository reference
repository.close()