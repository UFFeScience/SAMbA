package br.uff.spark.advancedpipe

import java.util

object FileGroupBuilder {
  def builder(): FileGroupBuilder = new FileGroupBuilder
}
/**
  * @author Thaylon Guedes Santos
  */
class FileGroupBuilder {

  private val data = new java.util.LinkedList[FileElement]
  private val extrasInfo = new util.HashMap[String, Any]()

  def putAll(map: java.util.Map[String, Any]): FileGroupBuilder = {
    extrasInfo.putAll(map)
    this
  }

  def put(key: String, value: Any): FileGroupBuilder = {
    extrasInfo.put(key, value)
    this
  }

  def addAll(fileGroup: FileGroup): FileGroupBuilder = {
    fileGroup.getFileElements.foreach(fileElement => add(fileElement))
    this
  }

  def add(fileElment: FileElement): FileGroupBuilder = {
    data.add(fileElment)
    this
  }

  def build(): FileGroup = {
    val result = FileGroup.of(data, false)
    result.setExtrasInfo(extrasInfo)
    result
  }

}
