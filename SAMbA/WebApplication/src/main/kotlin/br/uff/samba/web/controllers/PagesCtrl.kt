package br.uff.samba.web.controllers

import br.uff.samba.web.repository.ExecutionRepository
import br.uff.samba.web.repository.TaskRepository
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.servlet.ModelAndView
import java.util.*

@Controller
@RequestMapping("/")
class IndexCtrl(val executionRepo: ExecutionRepository) {

    @GetMapping
    fun index(): ModelAndView {
        val modelAndView = ModelAndView("index")
        modelAndView.addObject("executions", executionRepo.listAll().sortedByDescending {4
            it.StartTime
        })
        return modelAndView
    }
}

@Controller
@RequestMapping("/execution/fullgraph")
class FullGraphCtrl(val executionRepo: ExecutionRepository) {

    @GetMapping
    fun details(@RequestParam id: UUID): ModelAndView {
        val model = ModelAndView("execution/fullgraph")
        val execution = executionRepo.find(id)
        model.addObject("execution", execution)
        return model
    }

}


@Controller
@RequestMapping("/execution/details")
class DetailsCtrl(val executionRepo: ExecutionRepository,
                  val taskRepo: TaskRepository) {

    @GetMapping
    fun details(@RequestParam id: UUID): ModelAndView {
        val model = ModelAndView("execution/details")
        val execution = executionRepo.find(id)
        model.addObject("execution", execution)
        model.addObject("hasFiles", taskRepo.findAllTaskOfExecution(id).find { it.hasDataInRepository } != null)
        return model
    }

}


@Controller
@RequestMapping("/execution/taskgraph")
class TaskGraphCtrl(val executionRepo: ExecutionRepository,
                    var taskRepo: TaskRepository) {

    @GetMapping
    fun details(@RequestParam(name = "id") executionID: UUID, @RequestParam taskID: UUID): ModelAndView {
        val model = ModelAndView("execution/taskgraph")
        val execution = executionRepo.find(executionID)
        model.addObject("execution", execution)
        model.addObject("task", taskRepo.findTask(executionID, taskID))
        return model
    }

}

