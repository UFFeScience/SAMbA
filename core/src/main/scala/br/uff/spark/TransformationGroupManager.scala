package br.uff.spark

import org.apache.spark.rdd.RDD

class TransformationGroupManager(val task: Task) {

  var currentTransformationGroup: TransformationGroup = null

  /**
    * Mark this RDD as the start of a Transformation Group.
    *
    * @param newTransformationGroup
    * @return RDD[T]
    */
  def initTransformationGroup(newTransformationGroup: TransformationGroup): Unit = {
    if (currentTransformationGroup != null) {
      throw new IllegalStateException(
        "This task already have others Transformations Group, so you cannot start a new here."
      )
    }
    newTransformationGroup.initTasksIDS.add(task)
    currentTransformationGroup = newTransformationGroup
  }

  /**
    * Finish a Transformation Group that already started in previous Task.
    *
    * @param transformationGroup
    * @return RDD[T]
    */
  def finishTransformationGroup(transformationGroup: TransformationGroup, task: Task): Unit = {
    if (transformationGroup.finishTaskID != null) {
      val taskWhichFinishedTransGroup = transformationGroup.finishTaskID
      throw new IllegalStateException(
        s"The Transformation Group ${transformationGroup.name} already finished " +
          s"by Task ${taskWhichFinishedTransGroup.id}:${taskWhichFinishedTransGroup.description}"
      )
    }
    if (currentTransformationGroup != transformationGroup) {
      throw new IllegalStateException(
        s"This ${transformationGroup.name} are't in the previous Transformations Group of this task."
      )
    }
    currentTransformationGroup.intermediaryTasksIDS.remove(task)
    transformationGroup.finishTaskID = task
    transformationGroup.persistIt()
    currentTransformationGroup = null
  }

  def processDependenciesOfTransformationGroup(previousTransGroup: java.util.Set[TransformationGroup],
                                               previousTaskCount: Int,
                                               transformationGroupCount: Int
                                              ): Unit = {
    if (!previousTransGroup.isEmpty) {
      if(previousTaskCount!=transformationGroupCount){
        throw new IllegalStateException(
          "There is one or more of a transformation that doesn't have a Transformation Group."
        )
      }
      if (previousTransGroup.size > 1) {
        throw new IllegalStateException(
          "There are more than one transformation group unfinished in the previous Task \n" +
            "Previous Transformations: " + previousTransGroup
        )
      }
      currentTransformationGroup = previousTransGroup.iterator().next()
      currentTransformationGroup.intermediaryTasksIDS.add(task)
      task.transformation = currentTransformationGroup
    }
  }

  def hasACurrentTransformationGroup() = currentTransformationGroup != null

}
