package br.uff.spark.database

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util
import java.util.UUID
import java.util.function.Consumer

import br.uff.spark._

import scala.collection.JavaConverters._

class CassandraDBDao(val execution: Execution) extends DataBaseBasicMethods {

  val log = org.apache.log4j.Logger.getLogger(this.getClass)
  val con = DataSource.getConnection

  private val STMT_INSERT_TASK = con.prepare(
    """
      |INSERT INTO dfanalyzer."Task"
      |            ("executionID", id, description, "transformationType", "schemaFields", "usingDefaultSchema", "hasDataInRepository")
      |VALUES
      |            (?, ?, ?, ?, ?, ?, ?);
    """.stripMargin)

  private val STMT_DEPENDENCIES_OF_TASK = con.prepare(
    """
      |INSERT INTO dfanalyzer."DependenciesOfTask"
      |            ("executionID", target, "source")
      |VALUES
      |            (?, ?, ?);
    """.stripMargin)

  override def insertTask(task: Task): Unit = {
    var fields: java.util.List[String] = null

    fields = java.util.Arrays.asList(task.schema.getFieldsNames(): _*)

    con.executeAsync(
      STMT_INSERT_TASK.bind(
        execution.ID,
        task.id,
        task.description,
        task.transformationType.toString,
        fields,
        java.lang.Boolean.valueOf(task.usingDefaultSchema),
        java.lang.Boolean.valueOf(task.hasDataInRepository)
      )
    )

    con.executeAsync(
      STMT_DEPENDENCIES_OF_TASK.bind(
        execution.ID,
        task.id,
        task.dependenciesIDS.toSet.asJava
      )
    )
  }

  private val STMT_INSERT_FILE_GROUP_REFERENCE = con.prepare(
    """
      |INSERT INTO dfanalyzer."FileGroupReference"
      |            ("executionID", "id", "folderPath")
      |VALUES
      |            (?, ?, ?);
    """.stripMargin)

  override def insertFileGroupReference(dataElementID: UUID, folderPathInRepository: String): Unit = {
    con.executeAsync(
      STMT_INSERT_FILE_GROUP_REFERENCE.bind(
        execution.ID,
        dataElementID,
        folderPathInRepository
      )
    )
  }

  override def close(): Unit = {
    try {
      DataSource.close()
    } catch {
      case ex: Exception =>
        log.error("Error on try close the connection if database")
    }
  }

  val STMT_UPDATE_EXECUTION = con.prepare(
    """
      |UPDATE dfanalyzer."Execution"
      |SET    "EndTime" = ?
      |WHERE  id = ?;
    """.stripMargin)

  override def updateExecution(execution: Execution): Unit = {
    con.executeAsync(
      STMT_UPDATE_EXECUTION.bind(
        Timestamp.from(execution.endTime.toInstant(ZonedDateTime.now(ZoneId.systemDefault()).getOffset)),
        execution.ID
      )
    )

  }

  val STMT_INSERT_EXECUTION = con.prepare(
    """
      | INSERT INTO dfanalyzer."Execution"
      |            (id, "StartTime", "EndTime", "ApplicationName")
      |VALUES
      |            (?, ?, ?, ?);
    """.stripMargin)

  override def init(): Unit = {
    con.executeAsync(
      STMT_INSERT_EXECUTION.bind(
        execution.ID,
        Timestamp.from(execution.startTime.toInstant(ZonedDateTime.now(ZoneId.systemDefault()).getOffset)),
        null,
        execution.name
      )
    )
  }

  val STMT_INSERT_DATA_ELEMENT = con.prepare(
    """
      |INSERT INTO dfanalyzer."DataElement"
      |            (id, values, "executionID")
      |VALUES
      |            (?, ?, ?);
    """.stripMargin)

  override def insertDataElement(dataElement: DataElement[_ <: Any]): Unit = {
    con.executeAsync(
      STMT_INSERT_DATA_ELEMENT.bind(
        dataElement.id,
        dataElement.applySchemaToTheValue(),
        execution.ID
      )
    )

    if (!dataElement.dependenciesIDS.isEmpty)
      setDependencies(dataElement)
  }

  val STMT_INSERT_DEPENDENCIES_OF_DATA_ELEMENT = con.prepare(
    """
      | INSERT INTO dfanalyzer."DependenciesOfDataElement"
      |            (source, target, task, "executionID")
      |VALUES
      |            (?, ?, ?, ?);
    """.stripMargin)

  override def setDependencies(dataElement: DataElement[_ <: Any]): Unit = {
    con.executeAsync(
      STMT_INSERT_DEPENDENCIES_OF_DATA_ELEMENT.bind(
        new util.HashSet[UUID](dataElement.dependenciesIDS),
        dataElement.id,
        dataElement.task.id,
        execution.ID
      )
    )
  }

  val STMT_UPDATE_DEPENDENCIES_OF_DATA_ELEMENT = con.prepare(
    """
      |UPDATE dfanalyzer."DependenciesOfDataElement"
      |SET    source = source + ?
      |WHERE  target = ?
      |       AND task = ?
      |       AND "executionID" = ?
    """.stripMargin)

  override def insertDependencyOfDataElement(dataElement: DataElement[_ <: Any], id: UUID): Unit = {
    con.executeAsync(
      STMT_UPDATE_DEPENDENCIES_OF_DATA_ELEMENT.bind(
        Set(id).asJava,
        dataElement.id,
        dataElement.task.id,
        execution.ID
      )
    )
  }

  override def insertDependenciesOfDataElement(dataElement: DataElement[_], ids: util.List[UUID]): Unit = {
    ids.forEach(new Consumer[UUID] {
      override def accept(id: UUID): Unit = insertDependencyOfDataElement(dataElement, id)
    })
  }

  val STMT_INSERT_TRANSFORMATION_GROUP = con.prepare(
    """
      |INSERT INTO dfanalyzer."TransformationGroup"
      |            ("executionID", id, "initTasksIDS", "intermediaryTasksIDS", "finishTaskID")
      |VALUES
      |            (?, ?, ?, ?, ?);
    """.stripMargin)

  override def insertTransformationGroup(group: TransformationGroup): Unit = {
    con.executeAsync(
      STMT_INSERT_TRANSFORMATION_GROUP.bind(
        execution.ID,
        group.id,
        group.initTasksIDS,
        group.intermediaryTasksIDS,
        group.finishTaskID
      )
    )
  }

  val STMT_UPDATE_DATA_ELEMENT = con.prepare(
    """
      |UPDATE
      |   dfanalyzer."DataElement"
      |SET
      |   values = ?
      |WHERE
      |     "executionID"=?
      |  AND
      |     id=?;
    """.stripMargin)

  override def updateValueOfDataElement(dataElement: DataElement[_ <: Any]): Unit = {
    con.executeAsync(
      STMT_UPDATE_DATA_ELEMENT.bind(
        dataElement.applySchemaToTheValue(),
        execution.ID,
        dataElement.id
      )
    )
  }

  val STMT_DELETE_DEPENDENCIES_OF_DATA_ELEMENT = con.prepare(
    """
      |DELETE
      |FROM dfanalyzer."DependenciesOfDataElement"
      |WHERE
      |       "executionID"=?
      |  AND
      |       task=?
      |  AND
      |       target=?
    """.stripMargin)

  val STMT_DELETE_DATA_ELEMENT = con.prepare(
    """
      |DELETE
      |FROM dfanalyzer."DataElement"
      |WHERE
      |       "executionID"=?
      |  AND
      |       id=?
    """.stripMargin)

  override def deleteDataElement(dataElement: DataElement[_ <: Any]): Unit = {
    con.executeAsync(
      STMT_DELETE_DEPENDENCIES_OF_DATA_ELEMENT.bind(
        execution.ID,
        dataElement.task.id,
        dataElement.id
      )
    )

    con.executeAsync(
      STMT_DELETE_DATA_ELEMENT.bind(
        execution.ID,
        dataElement.id
      )
    )
  }

}
