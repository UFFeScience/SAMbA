package br.uff.samba.web.entities

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.cassandra.core.PrimaryKeyType
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn
import java.util.*

class DependenciesOfTask(
        @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED)
        val executionID: UUID,
        @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED)
        val target: UUID,
        val source: java.util.Set<UUID>?
) {
    fun buildJsonElement(mapper: ObjectMapper): List<JsonNode> {
        if (source == null) return Collections.emptyList()
        return source.map {
            val obj = mapper.createObjectNode()
            obj.put("from", it.toString())
            obj.put("to", target.toString())
        }
    }
}