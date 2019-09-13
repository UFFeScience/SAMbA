package br.uff.spark.schema

import scala.reflect.ClassTag

object DefaultArraySchema {
  def checkIfAInstanceOf(tag: ClassTag[_]): Boolean = {
    tag.runtimeClass.isArray
  }
}

class DefaultArraySchema[T] extends SingleLineSchema[T] {

  override def getFieldsNames(): Array[String] = {
    //    val result = Array.ofDim[String](fieldsSize)
    //    for (i <- 0 until fieldsSize) {
    //      result(i) = s"field $i"
    //    }
    //    return result
    Array("Array Schema")
  }

  override def splitData(value: T): Array[String] = value.asInstanceOf[Array[Any]].map(v => v.toString)
}
