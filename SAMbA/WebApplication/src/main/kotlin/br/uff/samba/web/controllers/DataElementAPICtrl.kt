package br.uff.samba.web.controllers

import br.uff.samba.web.allColors
import br.uff.samba.web.entities.DependenciesOfTask
import br.uff.samba.web.entities.Task
import br.uff.samba.web.enums.LabelType
import br.uff.samba.web.repository.*
import br.uff.samba.web.service.DataElementService
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import j2html.TagCreator.*
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.treewalk.TreeWalk
import org.eclipse.jgit.treewalk.filter.PathFilter
import org.springframework.core.io.InputStreamResource
import org.springframework.core.io.Resource
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.*
import java.io.File
import java.nio.file.Paths
import java.util.*
import kotlin.collections.HashMap


@Controller
@RequestMapping("/api/dataelement")
class DataElementAPICtrl(val dataElementService: DataElementService,
                         val taskRepo: TaskRepository,
                         val executionRepo: ExecutionRepository,
                         val dataElementRepo: DataElementRepository,
                         val fileGroupReferenceRepo: FileGroupReferenceRepository,
                         val dependenciesOfDataElementRepo: DependenciesOfDataElementRepository,
                         val dependenciesOfTaskRepo: DependenciesOfTaskRepository) {

    val mapper = ObjectMapper()

    @ResponseBody
    @GetMapping("/graph/{idExecution}", produces = arrayOf(MediaType.APPLICATION_JSON_UTF8_VALUE))
    fun getFullGraph(@PathVariable idExecution: UUID,
                     @RequestParam(defaultValue = "false") showCluster: Boolean,
                     @RequestParam(defaultValue = "ID") nodeLabelType: LabelType): String {


        val mapper = ObjectMapper()
        val nodes = mapper.createArrayNode()

        val allDataElements = dataElementService.getAll(idExecution)
                .map { it.id to it }
                .toMap().toMutableMap()

        val edgesBuilder = StringBuilder()
        var first = true

        var count = 0
        val taskLegend = HashMap<UUID, String>()
        val allLegend = mapper.createArrayNode()

        dependenciesOfDataElementRepo.findAll(idExecution).forEach { targetElement ->
            if (!allDataElements.containsKey(targetElement.target)) {
                println("DELETE FROM dfanalyzer.\"DataElement\" WHERE \"executionID\" = bc2ea50e-9374-46e1-9f11-efe5d47427ac AND id = ${targetElement.target};")
            }
            val dataElement = allDataElements[targetElement.target]!!
            if (!taskLegend.containsKey(targetElement.task)) {
                val newLegend = allLegend.addObject()
                val color = allColors[count++]
                taskLegend[targetElement.task] = color
                newLegend.put("shape", "box")
                newLegend.put("color", color)
                newLegend.put("physics", false)
                newLegend.put("fixed", true)
                newLegend.put("value", 1)
                newLegend.put("label", taskRepo.findTask(idExecution, targetElement.task).description)
            }
            dataElement.taskID = targetElement.task
            allDataElements.remove(targetElement.target)
            if (showCluster || !dataElement.toString().contains("pass")) {
                println("d: " + dataElement.toString())
                nodes.add(dataElement.buildJsonElement(mapper, nodeLabelType).put("color", taskLegend[dataElement.taskID!!].toString()))
                targetElement.source.forEach {
                    if (first) {
                        first = false
                    } else {
                        edgesBuilder.append(",")
                    }
                    edgesBuilder.append("{\"from\": \"$it\", \"to\": \"${targetElement.target}\"}")
                }
            }
        }
        allDataElements.forEach {
            val json = it.value.buildJsonElement(mapper, nodeLabelType)

            nodes.add(json)
        }
        val result = StringBuilder()
        result.append("{\"nodes\":").append(nodes.toString())
        result.append(", \"edges\":[").append(edgesBuilder).append("]")
        result.append(", \"legenda\": " + allLegend.toString() + " }")
        return result.toString()
    }

    @ResponseBody
    @GetMapping("/graphOfTask/{idExecution}", produces = arrayOf(MediaType.APPLICATION_JSON_UTF8_VALUE))
    fun getFullGraph(@PathVariable idExecution: UUID,
                     @RequestParam taskID: UUID,
                     @RequestParam(defaultValue = "false") showUsedData: Boolean,
                     @RequestParam(defaultValue = "VALUE") nodeLabelType: LabelType): JsonNode {

        val task = taskRepo.findTask(idExecution, taskID)

        var data = mapper.createObjectNode()
        var nodesArray = data.putArray("nodes")
        var edgesArray = data.putArray("edges")

        val allTask = mutableListOf<Task>()
        nodesArray.add(task.buildJsonElement(mapper).put("color", "#FB7E81"))

        if (task.transformationType == "UNION") {
            val dep = dependenciesOfTaskRepo.findDependenciesOfTask(idExecution, taskID)
            dep.source!!.forEach {
                var dependenciesTask = taskRepo.findTask(idExecution, it)
                allTask.add(dependenciesTask)
                nodesArray.add(dependenciesTask.buildJsonElement(mapper).put("color", "#FB7E81"))
            }
            edgesArray.addAll(dep.buildJsonElement(mapper))
        } else {
            allTask.add(task)
        }

        val allUsedAndGeneratedDE = mutableSetOf<Pair<UUID?, UUID>>()//TaskID - DataElementID

        val dependenciesOfTask: DependenciesOfTask? = if (showUsedData) dependenciesOfTaskRepo.findDependenciesOfTask(idExecution, taskID) else null

        allTask.forEach { currentTask ->
            dependenciesOfDataElementRepo.findAllDataElementProducedByTask(idExecution, currentTask.id)
                    .forEach { dependenciesOfDataElement ->
                        allUsedAndGeneratedDE.add(taskID to dependenciesOfDataElement.target)
                        edgesArray.add(
                                mapper.createObjectNode().put("from", task.id.toString()).put("to", dependenciesOfDataElement.target.toString())
                        )
                        if (showUsedData) {
                            dependenciesOfDataElement.source.forEach { sourceDE ->
                                allUsedAndGeneratedDE.add(findTask(idExecution, dependenciesOfTask, sourceDE) to sourceDE)
                                edgesArray.add(
                                        mapper.createObjectNode().put("from", sourceDE.toString()).put("to", dependenciesOfDataElement.target.toString())
                                )
                            }
                        }
                    }
        }
        allUsedAndGeneratedDE.forEach {
            var dataElement = dataElementService.loadDataElement(idExecution, it.second)
            dataElement.taskID = it.first
            val de = dataElement.buildJsonElement(mapper, nodeLabelType)
            if (it.first != null && it.first == taskID) {
                de.put("color", "#FFF")
                de.put("shadow", true)
            }
            nodesArray.add(de)
        }
        return data
    }

    private fun findTask(executionID: UUID, dependenciesOfTask: DependenciesOfTask?, dataElementId: UUID): UUID? {
        if (dependenciesOfTask == null || dependenciesOfTask.source == null)
            return null

        dependenciesOfTask!!.source!!.forEach { task ->
            val result = dependenciesOfDataElementRepo.findAllDataElementProducedByTask(executionID, task).anyMatch { it.target == dataElementId }
            if (result) {
                return task
            }
        }

        return null
    }

    @ResponseBody
    @GetMapping("/table/{dataElementId}", produces = [MediaType.APPLICATION_JSON_UTF8_VALUE])
    fun getDataElementId(@PathVariable dataElementId: UUID,
                         @RequestParam executionID: UUID,
                         @RequestParam(required = false) taskID: UUID?): String {
        val dataElement = dataElementService.loadDataElement(executionID, dataElementId)
        if (taskID == null) {
            val table = table(
                    thead(
                            tr(
                                    th("Value")
                            )
                    ),
                    tbody(
                            each(dataElement.values, { row ->
                                tr(
                                        each(row, { value ->
                                            td(value)
                                        })
                                )
                            })
                    )
            ).withClasses("table", "no-margin", "table-hover")
            val result = mapper.createObjectNode()
            result.put("tableContent", table.render())
            result.put("hasDataInRepository", false)
            return result.toString()
        }
        val task = taskRepo.findTask(executionID, taskID)
        val table = table(
                thead(
                        tr(
                                *task.schemaFields.map { th(it) }.toTypedArray()
                        )
                ),
                tbody(
                        each(dataElement.values, { row ->
                            tr(
                                    each(row, { value ->
                                        td(value)
                                    })
                            )
                        })
                )
        ).withClasses("table", "no-margin", "table-hover")
        val result = mapper.createObjectNode()
        result.put("tableContent", table.render())
        result.put("hasDataInRepository", task.hasDataInRepository)
        if (task.hasDataInRepository) {
            result.put("filesTree", buildFileTree(executionID, dataElementId))
        }
        return result.toString()
    }

    private fun buildFileTree(executionID: UUID, dataElementId: UUID?): ObjectNode {
        val execution = executionRepo.find(executionID)
        val repositoryLocation = File(File(System.getenv("SPARK_HOME") + "/gitblit/data/git/"), "${execution.ApplicationName}.git")


        val repository = Git.open(repositoryLocation).repository
        val branch = repository.resolve("refs/heads/${executionID}")
        val walk = RevWalk(repository)
        val commit = walk.parseCommit(branch)
        walk.dispose()
        val tw = TreeWalk(repository)
        tw.addTree(commit.tree)
        tw.isRecursive = true

        if (dataElementId != null) {
            val fileGroupRef = fileGroupReferenceRepo.get(executionID, dataElementId!!)
            tw.filter = PathFilter.create(fileGroupRef.folderPath)
        }

        val root = GitTree("${execution.ApplicationName}.git", mapper.createObjectNode(), false)
        root.nodeObj.putObject("state").put("selected", true)
        while (tw.next()) {
            val path = tw.pathString.split("/")
            var current: GitTree? = root
            for (i in 0..path.size - 2) {
                var achou = current!!.children[path[i]]
                if (achou == null) {
                    val novo = GitTree(path[i], mapper.createObjectNode(), false)
                    current.children[path[i]] = novo
                    current.childrenNode!!.add(novo.nodeObj)
                    achou = novo
                }
                current = achou
            }
            val novo = GitTree(path.last(), mapper.createObjectNode(), true)
            novo.nodeObj.putObject("data")
                    .put("filePath", tw.pathString.replace("/", "!"))
                    .put("executionID", executionID.toString())
            current!!.children[path.last()] = novo
            current.childrenNode!!.add(novo.nodeObj)
        }
        return root.nodeObj
    }

    @ResponseBody
    @GetMapping("/download")
    fun downloadFile(@RequestParam executionID: UUID,
                     @RequestParam filePath: String): ResponseEntity<Resource> {
        val execution = executionRepo.find(executionID)
        val repositoryLocation = File(File(System.getenv("SPARK_HOME") + "/gitblit/data/git/"), "${execution.ApplicationName}.git")

        val repository = Git.open(repositoryLocation).repository
        val branch = repository.resolve("refs/heads/${executionID}")
        val walk = RevWalk(repository)
        val commit = walk.parseCommit(branch)
        walk.dispose()
        val tw = TreeWalk(repository)
        tw.addTree(commit.tree)
        tw.isRecursive = true
        val _filePath = filePath.replace("!", "/")
        tw.filter = PathFilter.create(_filePath)

        while (tw.next()) {
            if (tw.pathString == _filePath)
                break
        }

        val objectId = tw.getObjectId(0)
        val loader = repository.open(objectId)

        val resource = InputStreamResource(loader.openStream())

        val fileName = Paths.get(_filePath).fileName

        return ResponseEntity.ok()
                .header("Content-Disposition", "attachment; filename=\"${fileName}\"")
                .header("Content-Length", loader.size.toString())
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .body(resource)
    }

    @ResponseBody
    @GetMapping("/repositoryFileTree/{executionId}", produces = [MediaType.APPLICATION_JSON_UTF8_VALUE])
    fun getRepositoryFileTree(@PathVariable executionId: UUID): String {
        return buildFileTree(executionId, null).toString()
    }
}

class GitTree(val text: String, val nodeObj: ObjectNode, val isFile: Boolean) {

    init {
        nodeObj.put("text", text)
        if (isFile)
            nodeObj.put("icon", "jstree-file")
    }

    val children: MutableMap<String, GitTree> = hashMapOf()
    val childrenNode = if (!isFile) nodeObj.putArray("children") else null
}
