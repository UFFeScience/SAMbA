package br.uff.samba.web.conf

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class LocalDateTimeFomatter : JsonSerializer<LocalDateTime>() {

    val dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy")
    val dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")
    val urlDateFormater = DateTimeFormatter.ofPattern("dd-MM-yyyy")
    val urlDateTimeFormater = DateTimeFormatter.ofPattern("hh:mm-dd-MM-yyyy")

    override fun serialize(value: LocalDateTime, gen: JsonGenerator, ser: SerializerProvider?) {
        gen.writeString(value.format(dateTimeFormatter))
    }

}