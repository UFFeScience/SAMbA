package br.uff.spark

import java.io.Serializable
import java.util.UUID

import br.uff.spark.TransformationType.TransformationType
import br.uff.spark.schema.DataElementSchema
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Task(@transient val rdd: RDD[_ <: Any]) extends Serializable {

  val id = UUID.randomUUID()
  var transformationType: TransformationType = TransformationType.UNKNOWN
  var isIgnored = false
  var alreadyPersisted = false
  var description: String = "TaskID_" + (if (rdd == null) "" else rdd.id) //This if is for run test without RDD
  var hasDataInRepository = false

  /* Schema of Data*/
  var usingDefaultSchema: Boolean = false
  var schema: DataElementSchema[_] = null
  var parseValue: (Any) => Array[Array[String]] = null

  /* Transfomation Group of Task*/
  var transformation: TransformationGroup = null

  /* Previous Tasks */
  val dependenciesIDS = new mutable.MutableList[UUID]()

  def addDependency(rdd: RDD[_ <: Any]): Task = addDependency(rdd.task)

  def addDependency(task: Task): Task = {
    if (task.isIgnored) {
      for (elem <- task.dependenciesIDS) {
        dependenciesIDS += elem
      }
    } else {
      dependenciesIDS += task.id
    }
    this
  }

  def checkAndPersist(): Unit = synchronized {
    if (!alreadyPersisted && !isIgnored) {
      DataflowProvenance.getInstance.add(this)
    }
    alreadyPersisted = true
  }

  override def toString = s"Task($id, $transformationType, $description ,$isIgnored)"
}
