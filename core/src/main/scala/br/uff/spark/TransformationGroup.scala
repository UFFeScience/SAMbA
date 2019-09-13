package br.uff.spark

import java.util
import java.util.UUID

class TransformationGroup(val name: String) extends Serializable {

  val id = UUID.randomUUID()

  val initTasksIDS = new util.HashSet[Task]()

  val intermediaryTasksIDS = new util.HashSet[Task]()

  var finishTaskID: Task = null

  def persistIt(): Unit = {
    DataflowProvenance.getInstance.add(this)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[TransformationGroup]

  override def equals(other: Any): Boolean = other match {
    case that: TransformationGroup =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = id.hashCode()


  override def toString = s"TransformationGroup($id, $name)"
}
