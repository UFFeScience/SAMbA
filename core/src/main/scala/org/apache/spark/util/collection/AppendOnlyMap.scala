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

package org.apache.spark.util.collection

import java.util.Comparator

import br.uff.spark.{DataElement, Task}
import com.google.common.hash.Hashing
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 *
 * This implementation uses quadratic probing with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 *
 * The map can support up to `375809638 (0.7 * 2 ^ 29)` elements.
 *
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 */
@DeveloperApi
class AppendOnlyMap[K, V](taskOfRDD:Task, initialCapacity: Int = 64)
  extends Iterable[DataElement[(K, V)]] with Serializable {
  import AppendOnlyMap._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  private val LOAD_FACTOR = 0.7

  private var capacity = nextPowerOf2(initialCapacity)
  private var mask = capacity - 1
  private var curSize = 0
  private var growThreshold = (LOAD_FACTOR * capacity).toInt

  // Holds keys and values in the same array for memory locality; specifically, the order of
  // elements is key0, value0, key1, value1, key2, value2, etc.
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  private var haveNullValue = false
  private var nullValue: DataElement[(Any, V)] = null.asInstanceOf[DataElement[(Any, V)]]

  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  private var destroyed = false
  private val destructionMessage = "Map state is invalid from destructive sorting!"

  /** Get the value for a given key */
  def apply(key: K): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      return if (nullValue == null) null.asInstanceOf[V] else nullValue.value._2
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        return data(2 * pos + 1).asInstanceOf[DataElement[(Any, V)]].value._2
      } else if (curKey.eq(null)) {
        return null.asInstanceOf[V]
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V]
  }

  /** Set the value for a key */
  def update(key: K, value: DataElement[_<:Any]): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = value.asInstanceOf[DataElement[(Any, V)]]
      haveNullValue = true
      return
    }
    var pos = rehash(key.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        data(2 * pos) = k
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        incrementSize()  // Since we added a new key
        return
      } else if (k.eq(curKey) || k.equals(curKey)) {
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /**
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   */
  def changeValue(key: K, realKey: Any, dataElement: DataElement[_ <: Any], alreadyExistsDataelement: DataElement[_ <: Any], updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      if (nullValue == null) {
        val newValue = updateFunc(haveNullValue, null.asInstanceOf[V])
        nullValue = if (alreadyExistsDataelement != null)
          alreadyExistsDataelement.updateValue((realKey, newValue)).asInstanceOf[DataElement[(Any, V)]]
        else
          DataElement.of((realKey, newValue), taskOfRDD, taskOfRDD.isIgnored, dataElement)
      } else {
        val newValue = updateFunc(haveNullValue, nullValue.value._2)
        nullValue.updateValue((realKey, newValue)) // Possivel gerador de lixo  UFF
        if (alreadyExistsDataelement == null) {
          nullValue.addDependency(dataElement)
        } else {
          nullValue.addDependencies(alreadyExistsDataelement.dependenciesIDS)
          alreadyExistsDataelement.deleteIt()
        }
      }
      haveNullValue = true
      return nullValue.value._2
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = if (alreadyExistsDataelement != null)
          alreadyExistsDataelement.updateValue((realKey, newValue))
        else
          DataElement.of((realKey, newValue), taskOfRDD, taskOfRDD.isIgnored, dataElement)
        incrementSize()
        return newValue
      } else if (k.eq(curKey) || k.equals(curKey)) {
        val entry: DataElement[(Any, V)] = data(2 * pos + 1).asInstanceOf[DataElement[(Any, V)]]
        val newValue = updateFunc(true, entry.value._2)
        entry.updateValue((realKey, newValue)) // Possivel gerador de lixo  UFF
        if (alreadyExistsDataelement == null) {
          entry.addDependency(dataElement)
        } else {
          entry.addDependencies(alreadyExistsDataelement.dependenciesIDS)
          alreadyExistsDataelement.deleteIt()
        }
        return newValue
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }

  /** Iterator method from Iterable */
  override def iterator: Iterator[DataElement[(K, V)]] = {
    assert(!destroyed, destructionMessage)
    new Iterator[DataElement[(K, V)]] {
      var pos = -1

      /** Get the next value we should return from next(), or null if we're finished iterating */
      def nextValue(): DataElement[(K, V)] = {
        if (pos == -1) {    // Treat position -1 as looking at the null value
          if (haveNullValue) {
            return nullValue.asInstanceOf[DataElement[(K, V)]]
          }
          pos += 1
        }
        while (pos < capacity) {
          if (!data(2 * pos).eq(null)) {
            return data(2 * pos + 1).asInstanceOf[DataElement[(K, V)]]
          }
          pos += 1
        }
        null
      }

      override def hasNext: Boolean = nextValue() != null

      override def next(): DataElement[(K, V)] = {
        val value = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator")
        }
        pos += 1
        value
      }
    }
  }

  override def size: Int = curSize

  /** Increase table size by 1, rehashing if necessary */
  private def incrementSize() {
    curSize += 1
    if (curSize > growThreshold) {
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   */
  private def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  /** Double the table's size and re-hash everything */
  protected def growTable() {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
    val newCapacity = capacity * 2
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
    val newData = new Array[AnyRef](2 * newCapacity)
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0
    while (oldPos < capacity) {
      if (!data(2 * oldPos).eq(null)) {
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
          val curKey = newData(2 * newPos)
          if (curKey.eq(null)) {
            newData(2 * newPos) = key
            newData(2 * newPos + 1) = value
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }
    data = newData
    capacity = newCapacity
    mask = newMask
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }

  /**
   * Return an iterator of the map in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.
   */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    var keyIndex, newIndex = 0
    while (keyIndex < capacity) {
      if (data(2 * keyIndex) != null) {
        data(2 * newIndex) = data(2 * keyIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))

    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)

    new Iterator[(K, V)] {
      var i = 0
      var nullValueReady = haveNullValue
      def hasNext: Boolean = (i < newIndex || nullValueReady)
      def next(): (K, V) = {
        if (nullValueReady) {
          nullValueReady = false
          (nullValue.value._1.asInstanceOf[K], nullValue.asInstanceOf[V])
        } else {
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }

  /**
   * Return whether the next insert will cause the map to grow
   */
  def atGrowThreshold: Boolean = curSize == growThreshold
}

private object AppendOnlyMap {
  val MAXIMUM_CAPACITY = (1 << 29)
}
