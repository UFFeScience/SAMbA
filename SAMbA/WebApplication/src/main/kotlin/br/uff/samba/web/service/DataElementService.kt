package br.uff.samba.web.service

import br.uff.samba.web.entities.DataElement
import com.datastax.driver.core.Session
import com.datastax.driver.core.TypeTokens
import org.springframework.stereotype.Service
import java.util.*

@Service
class DataElementService(val con: Session) {

    val STMT_GET_ALL_DATA_ELEMENT = con.prepare("select *  FROM  dfanalyzer.\"DataElement\" WHERE \"executionID\"=?;")
    fun getAll(executionID: UUID): Sequence<DataElement> {
        val stmt = STMT_GET_ALL_DATA_ELEMENT
                .bind(executionID)
        val rs = con.execute(stmt)
        val iter = rs.iterator()
        return iter.asSequence().map {
            DataElement(executionID, it.getUUID("id"), it.getList("values", TypeTokens.listOf(String::class.java)))
        }
    }

    val STMT_LOAD_DATA_ELEMENT = con.prepare("select *  FROM  dfanalyzer.\"DataElement\" WHERE \"executionID\"=? and id=?;")
    fun loadDataElement(executionID: UUID, dataElementID: UUID): DataElement {

        val stmt = STMT_LOAD_DATA_ELEMENT
                .bind(
                        executionID,
                        dataElementID
                )
        val rs = con.execute(stmt)
        val row = rs.iterator().next()
        return DataElement(executionID, row.getUUID("id"), row.getList("values", TypeTokens.listOf(String::class.java)))
    }
}