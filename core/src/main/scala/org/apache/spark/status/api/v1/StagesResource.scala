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
package org.apache.spark.status.api.v1

import java.util.{HashMap, List => JList, Locale}
import javax.ws.rs._
import javax.ws.rs.core.{Context, MediaType, MultivaluedMap, UriInfo}

import org.apache.spark.SparkException
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.status.api.v1.StageStatus._
import org.apache.spark.status.api.v1.TaskSorting._
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.ApiHelper._

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StagesResource extends BaseAppResource {

  @GET
  def stageList(@QueryParam("status") statuses: JList[StageStatus]): Seq[StageData] = {
    withUI(_.store.stageList(statuses))
  }

  @GET
  @Path("{stageId: \\d+}")
  def stageData(
      @PathParam("stageId") stageId: Int,
      @QueryParam("details") @DefaultValue("true") details: Boolean): Seq[StageData] = {
    withUI { ui =>
      val ret = ui.store.stageData(stageId, details = details)
      if (ret.nonEmpty) {
        ret
      } else {
        throw new NotFoundException(s"unknown stage: $stageId")
      }
    }
  }

  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}")
  def oneAttemptData(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @QueryParam("details") @DefaultValue("true") details: Boolean): StageData = withUI { ui =>
    try {
      ui.store.stageAttempt(stageId, stageAttemptId, details = details)._1
    } catch {
      case _: NoSuchElementException =>
        // Change the message depending on whether there are any attempts for the requested stage.
        val all = ui.store.stageData(stageId)
        val msg = if (all.nonEmpty) {
          val ids = all.map(_.attemptId)
          s"unknown attempt for stage $stageId.  Found attempts: [${ids.mkString(",")}]"
        } else {
          s"unknown stage: $stageId"
        }
        throw new NotFoundException(msg)
    }
  }

  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskSummary")
  def taskSummary(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @DefaultValue("0.05,0.25,0.5,0.75,0.95") @QueryParam("quantiles") quantileString: String)
  : TaskMetricDistributions = withUI { ui =>
    val quantiles = quantileString.split(",").map { s =>
      try {
        s.toDouble
      } catch {
        case nfe: NumberFormatException =>
          throw new BadParameterException("quantiles", "double", s)
      }
    }

    ui.store.taskSummary(stageId, stageAttemptId, quantiles).getOrElse(
      throw new NotFoundException(s"No tasks reported metrics for $stageId / $stageAttemptId yet."))
  }

  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskList")
  def taskList(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("20") @QueryParam("length") length: Int,
      @DefaultValue("ID") @QueryParam("sortBy") sortBy: TaskSorting): Seq[TaskData] = {
    withUI(_.store.taskList(stageId, stageAttemptId, offset, length, sortBy))
  }

  // This api needs to stay formatted exactly as it is below, since, it is being used by the
  // datatables for the stages page.
  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskTable")
  def taskTable(
    @PathParam("stageId") stageId: Int,
    @PathParam("stageAttemptId") stageAttemptId: Int,
    @QueryParam("details") @DefaultValue("true") details: Boolean,
    @Context uriInfo: UriInfo):
  HashMap[String, Object] = {
    withUI { ui =>
      val uriQueryParameters = uriInfo.getQueryParameters(true)
      val totalRecords = uriQueryParameters.getFirst("numTasks")
      var isSearch = false
      var searchValue: String = null
      var filteredRecords = totalRecords
      // The datatables client API sends a list of query parameters to the server which contain
      // information like the columns to be sorted, search value typed by the user in the search
      // box, pagination index etc. For more information on these query parameters,
      // refer https://datatables.net/manual/server-side.
      if (uriQueryParameters.getFirst("search[value]") != null &&
        uriQueryParameters.getFirst("search[value]").length > 0) {
        isSearch = true
        searchValue = uriQueryParameters.getFirst("search[value]")
      }
      val _tasksToShow: Seq[TaskData] = doPagination(uriQueryParameters, stageId, stageAttemptId,
        isSearch, totalRecords.toInt)
      val ret = new HashMap[String, Object]()
      if (_tasksToShow.nonEmpty) {
        // Performs server-side search based on input from user
        if (isSearch) {
          val filteredTaskList = filterTaskList(_tasksToShow, searchValue)
          filteredRecords = filteredTaskList.length.toString
          if (filteredTaskList.length > 0) {
            val pageStartIndex = uriQueryParameters.getFirst("start").toInt
            val pageLength = uriQueryParameters.getFirst("length").toInt
            ret.put("aaData", filteredTaskList.slice(
              pageStartIndex, pageStartIndex + pageLength))
          } else {
            ret.put("aaData", filteredTaskList)
          }
        } else {
          ret.put("aaData", _tasksToShow)
        }
      } else {
        ret.put("aaData", _tasksToShow)
      }
      ret.put("recordsTotal", totalRecords)
      ret.put("recordsFiltered", filteredRecords)
      ret
    }
  }

  // Performs pagination on the server side
  def doPagination(queryParameters: MultivaluedMap[String, String], stageId: Int,
    stageAttemptId: Int, isSearch: Boolean, totalRecords: Int): Seq[TaskData] = {
    var columnNameToSort = queryParameters.getFirst("columnNameToSort")
    // Sorting on Logs column will default to Index column sort
    if (columnNameToSort.equalsIgnoreCase("Logs")) {
      columnNameToSort = "Index"
    }
    val isAscendingStr = queryParameters.getFirst("order[0][dir]")
    var pageStartIndex = 0
    var pageLength = totalRecords
    // We fetch only the desired rows upto the specified page length for all cases except when a
    // search query is present, in that case, we need to fetch all the rows to perform the search
    // on the entire table
    if (!isSearch) {
      pageStartIndex = queryParameters.getFirst("start").toInt
      pageLength = queryParameters.getFirst("length").toInt
    }
    withUI(_.store.taskList(stageId, stageAttemptId, pageStartIndex, pageLength,
      indexName(columnNameToSort), isAscendingStr.equalsIgnoreCase("asc")))
  }

  // Filters task list based on search parameter
  def filterTaskList(
    taskDataList: Seq[TaskData],
    searchValue: String): Seq[TaskData] = {
    val defaultOptionString: String = "d"
    val searchValueLowerCase = searchValue.toLowerCase(Locale.ROOT)
    val containsValue = (taskDataParams: Any) => taskDataParams.toString.toLowerCase(
      Locale.ROOT).contains(searchValueLowerCase)
    val taskMetricsContainsValue = (task: TaskData) => task.taskMetrics match {
      case None => false
      case Some(metrics) =>
        (containsValue(task.taskMetrics.get.executorDeserializeTime)
        || containsValue(task.taskMetrics.get.executorRunTime)
        || containsValue(task.taskMetrics.get.jvmGcTime)
        || containsValue(task.taskMetrics.get.resultSerializationTime)
        || containsValue(task.taskMetrics.get.memoryBytesSpilled)
        || containsValue(task.taskMetrics.get.diskBytesSpilled)
        || containsValue(task.taskMetrics.get.peakExecutionMemory)
        || containsValue(task.taskMetrics.get.inputMetrics.bytesRead)
        || containsValue(task.taskMetrics.get.inputMetrics.recordsRead)
        || containsValue(task.taskMetrics.get.outputMetrics.bytesWritten)
        || containsValue(task.taskMetrics.get.outputMetrics.recordsWritten)
        || containsValue(task.taskMetrics.get.shuffleReadMetrics.fetchWaitTime)
        || containsValue(task.taskMetrics.get.shuffleReadMetrics.recordsRead)
        || containsValue(task.taskMetrics.get.shuffleWriteMetrics.bytesWritten)
        || containsValue(task.taskMetrics.get.shuffleWriteMetrics.recordsWritten)
        || containsValue(task.taskMetrics.get.shuffleWriteMetrics.writeTime))
    }
    val filteredTaskDataSequence: Seq[TaskData] = taskDataList.filter(f =>
      (containsValue(f.taskId) || containsValue(f.index) || containsValue(f.attempt)
        || containsValue(f.launchTime)
        || containsValue(f.resultFetchStart.getOrElse(defaultOptionString))
        || containsValue(f.executorId) || containsValue(f.host) || containsValue(f.status)
        || containsValue(f.taskLocality) || containsValue(f.speculative)
        || containsValue(f.errorMessage.getOrElse(defaultOptionString))
        || taskMetricsContainsValue(f)
        || containsValue(f.schedulerDelay) || containsValue(f.gettingResultTime)))
    filteredTaskDataSequence
  }

}
