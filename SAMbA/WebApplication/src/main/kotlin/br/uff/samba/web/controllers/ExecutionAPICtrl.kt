package br.uff.samba.web.controllers

import br.uff.samba.web.entities.Execution
import br.uff.samba.web.repository.ExecutionRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseBody
import java.util.*

@Controller
@RequestMapping("/api/execution/")
class ExecutionAPICtrl {

    @Autowired private lateinit var executionRepo: ExecutionRepository

    @GetMapping("/list", produces = arrayOf(MediaType.APPLICATION_JSON_UTF8_VALUE))
    @ResponseBody
    fun listAll(): List<Execution> = executionRepo.listAll()

    @GetMapping("/find/{id}", produces = arrayOf(MediaType.APPLICATION_JSON_UTF8_VALUE))
    @ResponseBody
    fun listFind(@PathVariable id: UUID): Execution = executionRepo.find(id)

}