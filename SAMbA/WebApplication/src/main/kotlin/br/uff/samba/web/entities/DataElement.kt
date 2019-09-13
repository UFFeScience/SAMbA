package br.uff.samba.web.entities

import br.uff.samba.web.enums.LabelType
import com.datastax.driver.core.TypeTokens
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.springframework.cassandra.core.PrimaryKeyType
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.mapping.Table
import java.util.*

@Table("DataElement")
data class DataElement(
        @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED)
        val executionID: UUID,
        @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED)
        val id: UUID,
        val values: List<List<String>>
) {

    @Transient
    var taskID: UUID? = null

    fun buildJsonElement(mapper: ObjectMapper, labelType: LabelType): ObjectNode {

        TypeTokens.listOf(java.lang.String::class.java)
        val obj = mapper.createObjectNode()
        val label = when (labelType) {
            LabelType.ID -> id.toString()
            LabelType.VALUE -> values.joinToString("\n")
        }
        obj.put("id", id.toString())
        obj.put("label", label)
        obj.put("shape", "box")
        obj.put("taskID", if (taskID == null) null else taskID.toString())
        if (taskID == null) {
            obj.put("color", "#FFF")
            obj.put("shadow", true)
        }
        return obj
    }
}