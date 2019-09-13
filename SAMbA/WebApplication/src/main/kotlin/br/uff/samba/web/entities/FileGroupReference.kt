package br.uff.samba.web.entities

import org.springframework.cassandra.core.PrimaryKeyType
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.mapping.Table
import java.util.*

@Table("FileGroupReference")
class FileGroupReference(
        @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED)
        val executionID: UUID,
        @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED)
        val id: UUID,
        val folderPath: String
)