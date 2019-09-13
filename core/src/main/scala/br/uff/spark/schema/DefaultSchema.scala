package br.uff.spark.schema

class DefaultSchema[T <: Any] extends DataElementSchema[T] {

  override def getFieldsNames(): Array[String] = Array("Value")

  override def getSplittedData(value: T) = Array(Array(value.toString))

}
