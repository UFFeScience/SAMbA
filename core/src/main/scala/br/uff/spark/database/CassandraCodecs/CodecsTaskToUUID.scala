package br.uff.spark.database.CassandraCodecs

import java.util.UUID

import br.uff.spark.Task
import com.datastax.driver.core.TypeCodec
import com.datastax.driver.extras.codecs.MappingCodec


class CodecsTaskToUUID extends MappingCodec[Task, UUID](TypeCodec.uuid(), classOf[Task]) {

  override def serialize(o: Task) = o.id

  override def deserialize(i: UUID) = ???
}
