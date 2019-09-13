package br.uff.samba.web.repository

import br.uff.samba.web.entities.DataElement
import org.springframework.data.cassandra.repository.Query
import org.springframework.data.repository.CrudRepository
import java.util.*
import java.util.stream.Stream

interface DataElementRepository : CrudRepository<DataElement, String> {

    @Query("select *  FROM  dfanalyzer.\"DataElement\" WHERE \"executionID\"=?0;")
    fun findAll(idExecution: UUID): Stream<DataElement>

    @Query("select *  FROM  dfanalyzer.\"DataElement\" WHERE \"executionID\"=?0 and id=?1;")
    fun loadDataElement(idExecution: UUID, target: UUID): DataElement

}