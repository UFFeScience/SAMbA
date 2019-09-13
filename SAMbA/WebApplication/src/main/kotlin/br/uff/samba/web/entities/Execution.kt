package br.uff.samba.web.entities

import org.springframework.data.cassandra.mapping.PrimaryKey
import org.springframework.data.cassandra.mapping.Table
import java.time.LocalDateTime
import java.util.*

@Table(value = "Execution")
class Execution(
        @PrimaryKey
        val id: UUID,
        val ApplicationName: String,
        val StartTime: LocalDateTime,
        val EndTime: LocalDateTime?
)