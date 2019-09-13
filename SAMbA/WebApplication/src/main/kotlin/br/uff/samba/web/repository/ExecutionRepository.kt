package br.uff.samba.web.repository

import br.uff.samba.web.entities.Execution
import org.springframework.data.cassandra.repository.Query
import org.springframework.data.repository.CrudRepository
import java.util.*

interface ExecutionRepository : CrudRepository<Execution, String> {

    @Query("SELECT id, \"ApplicationName\", \"EndTime\", \"StartTime\" FROM dfanalyzer.\"Execution\";")
    fun listAll(): List<Execution>;

    @Query("SELECT id, \"ApplicationName\", \"EndTime\", \"StartTime\" FROM dfanalyzer.\"Execution\" where id=?0;")
    fun find(id: UUID): Execution

}