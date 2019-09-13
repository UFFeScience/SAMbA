package br.uff.samba.web.repository

import br.uff.samba.web.entities.DependenciesOfTask
import org.springframework.data.cassandra.repository.Query
import org.springframework.data.repository.CrudRepository
import java.util.*

interface DependenciesOfTaskRepository : CrudRepository<DependenciesOfTask, String> {

    @Query("SELECT * FROM dfanalyzer.\"DependenciesOfTask\" where  \"executionID\"=?0;")
    fun findAll(executionID: UUID): List<DependenciesOfTask>

    @Query("SELECT * FROM dfanalyzer.\"DependenciesOfTask\" where  \"executionID\"=?0 and target=?1;")
    fun findDependenciesOfTask(executionID: UUID, taskID: UUID): DependenciesOfTask

}