package br.uff.samba.web.repository

import br.uff.samba.web.entities.DependenciesOfDataElement
import org.springframework.data.cassandra.repository.Query
import org.springframework.data.repository.CrudRepository
import java.util.*
import java.util.stream.Stream

interface DependenciesOfDataElementRepository : CrudRepository<DependenciesOfDataElement, String> {
    @Query("SELECT * from dfanalyzer.\"DependenciesOfDataElement\" WHERE \"executionID\" = ?0")
    fun findAll(idExecution: UUID): Stream<DependenciesOfDataElement>

    @Query("SELECT * from dfanalyzer.\"DependenciesOfDataElement\" WHERE \"executionID\" = ?0 and task=?1")
    fun findAllDataElementProducedByTask(idExecution: UUID, taskID: UUID): Stream<DependenciesOfDataElement>
}