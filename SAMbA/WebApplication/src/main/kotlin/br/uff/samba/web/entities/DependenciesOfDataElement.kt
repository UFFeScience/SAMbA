package br.uff.samba.web.entities

import org.springframework.cassandra.core.PrimaryKeyType
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.mapping.Table
import java.util.*

@Table("DependenciesOfDataElement")
class DependenciesOfDataElement(
        @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED)
        val executionID: UUID,
        @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED)
        val task: UUID,
        @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED)
        val target: UUID,
        val source: java.util.Set<UUID>
)