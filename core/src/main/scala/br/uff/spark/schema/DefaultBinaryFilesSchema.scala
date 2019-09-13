package br.uff.spark.schema

import org.apache.spark.input.PortableDataStream

class DefaultBinaryFilesSchema extends SingleLineSchema[(String, PortableDataStream)] {
  override def splitData(value: (String, PortableDataStream)): Array[String] = Array(value._1.split(":").apply(1))

  override def getFieldsNames(): Array[String] = Array("File Path")
}
