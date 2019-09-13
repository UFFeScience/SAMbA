package br.uff.spark.schema

import scala.reflect.ClassTag


object DefaultPairSchema {

  val product2 = classOf[Product2[Any, Any]]
  val tuple = classOf[(Any, Any)]

  def checkIfAInstanceOf(tag: ClassTag[_]): Boolean = {
    val runtimeClass = tag.runtimeClass
    runtimeClass == product2 || runtimeClass == tuple
  }
}

class DefaultPairSchema[T] extends SingleLineSchema[T] {

  override def getFieldsNames(): Array[String] = Array("Key", "Value")

  override def splitData(value: T): Array[String] = {
    val _value = value.asInstanceOf[Product2[Any, Any]]
    Array(_value._1.toString, _value._2.toString)
  }

}
