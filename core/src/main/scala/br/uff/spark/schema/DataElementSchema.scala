package br.uff.spark.schema

trait DataElementSchema[T] extends Serializable {

  def getFieldsNames(): Array[String]

  def getSplittedData(value: T): Array[Array[String]]

}