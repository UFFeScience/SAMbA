package br.uff.spark.database

import java.util.UUID

import br.uff.spark.{DataElement, Execution, Task, TransformationGroup}

trait DataBaseBasicMethods {

  /* Basics Operations*/
  def init(): Unit = {}

  def close(): Unit = {}

  /* Insert Operations */

  def insertTask(task: Task): Unit = {}

  def insertTransformationGroup(group: TransformationGroup): Unit = {}

  def insertDataElement(dataElement: DataElement[_ <: Any]): Unit = {}

  def setDependencies(dataElement: DataElement[_ <: Any]): Unit = {}

  def insertDependencyOfDataElement(dataElement: DataElement[_ <: Any], id: UUID): Unit = {}

  def insertDependenciesOfDataElement(dataElement: DataElement[_ <: Any], ids: java.util.List[UUID]): Unit = {}

  def insertFileGroupReference(dataElementID: UUID, folderPathInRepository: String): Unit = {}

  /* Update operations*/
  def updateExecution(execution: Execution): Unit = {}

  def updateValueOfDataElement(dataElement: DataElement[_ <: Any]): Unit = {}

  /* Delete Operations*/
  def deleteDataElement(dataElement: DataElement[_ <: Any]): Unit = {}

  /* Utils Operations */
  def allFilesOfExecution(id: UUID, onRead: (String, String) => Unit): Unit = {}

  def allRelationshipBetweenDataElement(id: UUID, onRead: (String, String) => Unit): Unit = {}
}
