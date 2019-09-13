package br.uff.samba.web.repository

import br.uff.samba.web.entities.FileGroupReference
import org.springframework.data.cassandra.repository.Query
import org.springframework.data.repository.CrudRepository
import java.util.*

interface FileGroupReferenceRepository : CrudRepository<FileGroupReference, String> {

    @Query("select * from \"FileGroupReference\" where \"executionID\"=?0 and id=?1;")
    fun get(executionID: UUID, dataElementID: UUID): FileGroupReference

}