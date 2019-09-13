package br.uff.samba.web.entities

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.springframework.cassandra.core.PrimaryKeyType
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.mapping.Table
import java.util.*

@Table("Task")
class Task(
        @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED)
        val executionID: UUID,
        @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED)
        val id: UUID,
        val description: String,
        val transformationType: String,
        val usingDefaultSchema: Boolean,
        val schemaFields: List<String>,
        val hasDataInRepository: Boolean) {

    fun buildJsonElement(mapper: ObjectMapper): ObjectNode {
        val obj = mapper.createObjectNode()
        obj.put("id", this.id.toString())
        obj.put("executionID", this.executionID.toString())
        obj.put("label", "${this.transformationType}: ${this.description}")
        obj.put("shape", "box")
        return obj
    }

    fun buildAllFieldsJsonElement(mapper: ObjectMapper): ObjectNode {
        val obj = mapper.createObjectNode()
        obj.put("id", this.id.toString())
        obj.put("description", this.description)
        obj.put("transformationType", transformationType)
        return obj
    }
}