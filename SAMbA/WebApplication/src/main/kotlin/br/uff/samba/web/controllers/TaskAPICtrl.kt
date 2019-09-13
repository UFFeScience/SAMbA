package br.uff.samba.web.controllers

import br.uff.samba.web.repository.DependenciesOfTaskRepository
import br.uff.samba.web.repository.TaskRepository
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.*
import java.util.*


@Controller
@RequestMapping("/api/task")
class TaskAPICtrl {

    val mapper = ObjectMapper()
    @Autowired private lateinit var taskRepo: TaskRepository
    @Autowired private lateinit var dependenciesOfTaskRepo: DependenciesOfTaskRepository

    @GetMapping("/graph/{executionID}", produces = arrayOf(MediaType.APPLICATION_JSON_UTF8_VALUE))
    @ResponseBody
    fun getGraph(@PathVariable executionID: UUID): ObjectNode {
        val allTasks = taskRepo.findAllTaskOfExecution(executionID)
        val allDependencies = dependenciesOfTaskRepo.findAll(executionID)
        val data = mapper.createObjectNode()
        val nodesArray = data.putArray("nodes")

        allTasks.forEach({ nodesArray.add(it.buildJsonElement(mapper)) })

        val edgesArray = data.putArray("edges")
        allDependencies.forEach { edgesArray.addAll(it.buildJsonElement(mapper)) }
        return data
    }

    @GetMapping("/info/{executionID}", produces = arrayOf(MediaType.APPLICATION_JSON_UTF8_VALUE))
    @ResponseBody
    fun getTaskInfoWithGraph(@PathVariable executionID: UUID, @RequestParam taskID: UUID): ObjectNode {

        val task = taskRepo.findTask(executionID, taskID)
        val result = mapper.createObjectNode()

        result.put("info", task.buildAllFieldsJsonElement(mapper))
        val data = result.putObject("data")

        val nodesArray = data.putArray("nodes")
        nodesArray.add(task.buildJsonElement(mapper))

        // Loading edges
        val edgesArray = data.putArray("edges")
        val dependencie = dependenciesOfTaskRepo.findDependenciesOfTask(executionID, taskID)
        edgesArray.addAll(dependencie.buildJsonElement(mapper))
        if (dependencie.source != null) {
            dependencie.source.forEach {
                nodesArray.add(taskRepo.findTask(executionID, it).buildJsonElement(mapper))
            }
        }

        return result
    }
}