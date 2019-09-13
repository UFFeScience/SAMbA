package br.uff.spark.schema

import br.uff.spark.advancedpipe.FileGroup

import scala.reflect.ClassTag

object DefaultFileGroupSchema {

  var fileGroupClass = classOf[FileGroup]

  def checkIfAInstanceOf(tag: ClassTag[_]): Boolean = tag.runtimeClass == fileGroupClass
}

class DefaultFileGroupSchema[T] extends DataElementSchema[T] {
  override def getFieldsNames(): Array[String] = Array("Name", "Path", "Size")

  override def getSplittedData(value: T): Array[Array[String]] = {
    val _value = value.asInstanceOf[FileGroup]
    val result = new Array[Array[String]](_value.getFileElements.length)
    var index = 0
    for (elem <- _value.getFileElements) {
      result.update(index, Array(elem.getFileName, elem.getFilePath, elem.getFileSize.toString))
      index += 1
    }
    result
  }
}
