package br.uff.samba.web.repository

import br.uff.samba.web.entities.Task
import org.springframework.data.cassandra.repository.Query
import org.springframework.data.repository.CrudRepository
import java.util.*

interface TaskRepository : CrudRepository<Task, String> {

    @Query("SELECT * FROM dfanalyzer.\"Task\" where \"executionID\"=?0;")
    fun findAllTaskOfExecution(executionID: UUID): List<Task>

    @Query("SELECT * FROM dfanalyzer.\"Task\" where \"executionID\"=?0 and id=?1;")
    fun findTask(executionID: UUID, id: UUID): Task
}