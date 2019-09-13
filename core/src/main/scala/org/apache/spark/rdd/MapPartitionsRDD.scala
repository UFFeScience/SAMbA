/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import br.uff.spark.{DataElement, Task, TransformationType}

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

import scala.collection.AbstractIterator
import scala.collection.Iterator.empty

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 *
 * @param prev the parent RDD.
 * @param f The function used to map a tuple of (TaskContext, partition index, input iterator) to
 *          an output iterator.
 * @param preservesPartitioning Whether the input function preserves the partitioner, which should
 *                              be `false` unless `prev` is a pair RDD and the input function
 *                              doesn't modify the keys.
 * @param isFromBarrier Indicates whether this RDD is transformed from an RDDBarrier, a stage
 *                      containing at least one RDDBarrier shall be turned into a barrier stage.
 * @param isOrderSensitive whether or not the function is order-sensitive. If it's order
 *                         sensitive, it may return totally different result when the input order
 *                         is changed. Mostly stateful functions are order-sensitive.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (Task, TaskContext, Int, Iterator[DataElement[T]]) => Iterator[DataElement[U]],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[DataElement[U]] =
    f(this.task, context, split.index, firstParent[T].iterator(split, context))

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  @transient protected lazy override val isBarrier_ : Boolean =
    isFromBarrier || dependencies.exists(_.rdd.isBarrier())

  override protected def getOutputDeterministicLevel = {
    if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
  }
}

private[spark] class FilterMapPartitionsRDD[T: ClassTag](
  prev: RDD[T],
  f: T => Boolean,
  preservesPartitioning: Boolean = false) extends MapPartitionsRDD[T, T](prev, null, preservesPartitioning) {

  setTransformationType(TransformationType.FILTER)
  val ff = (x: DataElement[T]) => f.apply(x.value)
  var defaultNotPassValue: DataElement[String] = null

  override def checkAndPersistProvenance(): RDD[T] = {
    defaultNotPassValue = DataElement.ignoringSchemaOf("don't-pass: " + task.description, task, task.isIgnored)
    super.checkAndPersistProvenance()
    this
  }

  override def compute(split: Partition, context: TaskContext): Iterator[DataElement[T]] = {
    val input = firstParent[T].iterator(split, context)
    /** Returns an iterator over all the elements of this iterator that satisfy the predicate `p`.
      * The order of the elements is preserved.
      *
      * @param p the predicate used to test values.
      * @return an iterator which produces those values of this iterator which satisfy the predicate `p`.
      * @note Reuse: $consumesAndProducesIterator
      *
      * copied by Thaylon from: scala.collection.Iterator def filter(p: A => Boolean): Iterator[A]
      **/
    new AbstractIterator[DataElement[T]] {
      // TODO 2.12 - Make a full-fledged FilterImpl that will reverse sense of p
      private var hd: DataElement[T] = _
      private var hdDefined: Boolean = false

      def hasNext: Boolean = hdDefined || {
        var found = false
        do {
          if (!input.hasNext) return false
          hd = input.next()
          found = ff(hd)
          if (!found) {
            defaultNotPassValue.addDependency(hd)
          }
        } while (!found)
        hdDefined = true
        true
      }

      def next() = if (hasNext) {
        hdDefined = false;
        DataElement.of(hd.value, task, task.isIgnored, hd)
      } else empty.next()
    }
  }
}