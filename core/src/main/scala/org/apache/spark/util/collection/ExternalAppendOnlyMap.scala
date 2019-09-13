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

import java.io._
import java.util.{Comparator, UUID}

import br.uff.spark.{DataElement, DataflowUtils, Task}
import com.google.common.io.ByteStreams

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, Serializer, SerializerManager}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalAppendOnlyMap.HashComparator
import org.apache.spark.{SparkEnv, TaskContext}

import scala.collection.{BufferedIterator, mutable}
import scala.collection.mutable.ArrayBuffer

/**
 * :: DeveloperApi ::
 * An append-only map that spills sorted content to disk when there is insufficient space for it
 * to grow.
 *
 * This map takes two passes over the data:
 *
 *   (1) Values are merged into combiners, which are sorted and spilled to disk as necessary
 *   (2) Combiners are read from disk and merged together
 *
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is
 * too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary disk
 * writes. This may lead to a performance regression compared to the normal case of using the
 * non-spilling AppendOnlyMap.
 */
@DeveloperApi
class ExternalAppendOnlyMap[K, V, C](
    taskOfRDD:Task,
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    context: TaskContext = TaskContext.get(),
    serializerManager: SerializerManager = SparkEnv.get.serializerManager)
  extends Spillable[SizeTracker](context.taskMemoryManager())
  with Serializable
  with Logging
  with Iterable[DataElement[(K, C)]] {

  if (context == null) {
    throw new IllegalStateException(
      "Spillable collections should not be instantiated outside of tasks")
  }

  // Backwards-compatibility constructor for binary compatibility
  def this(
      taskOfRDD:Task,
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      serializer: Serializer,
      blockManager: BlockManager) {
    this(taskOfRDD,createCombiner, mergeValue, mergeCombiners, serializer, blockManager, TaskContext.get())
  }

  /**
   * Exposed for testing
   */
  @volatile private[collection] var currentMap = new SizeTrackingAppendOnlyMap[K, C](taskOfRDD)
  private val spilledMaps = new ArrayBuffer[DiskMapIterator]
  private val sparkConf = SparkEnv.get.conf
  private val diskBlockManager = blockManager.diskBlockManager

  /**
   * Size of object batches when reading/writing from serializers.
   *
   * Objects are written in batches, with each batch using its own serialization stream. This
   * cuts down on the size of reference-tracking maps constructed when deserializing a stream.
   *
   * NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
   * grow internal data structures by growing + copying every time the number of objects doubles.
   */
  private val serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Number of bytes spilled in total
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize =
    sparkConf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Write metrics
  private val writeMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  // Peak size of the in-memory map observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  private val keyComparator = new HashComparator[K]
  private val ser = serializer.newInstance()

  @volatile private var readingIterator: SpillableIterator = null

  /**
   * Number of files this map has spilled so far.
   * Exposed for testing.
   */
  private[collection] def numSpills: Int = spilledMaps.size

  /**
   * Insert the given key and value into the map.
   */
  def insert(key: K, value: V, dependencyDE: DataElement[_ <: Any]): Unit = {
    var dataElement = if (dependencyDE == null)
      DataElement.of((key, value), taskOfRDD, taskOfRDD.isIgnored)
    else
      DataElement.of((key, value), taskOfRDD, taskOfRDD.isIgnored, dependencyDE)
    insertAll(Iterator(dataElement))
  }

  /**
    * Classe criada para obter metadados sobre o objeto corrente, pois ele varia
    * @param curEntry
    * @tparam K
    * @tparam V
    */
  class EntriesMetaData[K, V](curEntry: Any, task: Task) {

    var alreadyExistsDataelement: DataElement[Product2[K, V]] = null
    var key: K = null.asInstanceOf[K]
    var dependencyID: DataElement[_ <: Any] = null //.asInstanceOf[String]

    val value: V = if (curEntry.isInstanceOf[DataElement[_ <: Any]]) { // when data is coming from the another RDD
      key = curEntry.asInstanceOf[DataElement[(K, V)]].value._1
      dependencyID = curEntry.asInstanceOf[DataElement[(K, V)]]
      curEntry.asInstanceOf[DataElement[(K, V)]].value._2
    } else { // when data is coming from the serialized file
      val externalSortDataElementTuple = curEntry.asInstanceOf[Product2[K, V]]._2.asInstanceOf[DataElement[Product2[K, V]]]
      if (externalSortDataElementTuple.task.id == task.id)
        alreadyExistsDataelement = externalSortDataElementTuple
      key = curEntry.asInstanceOf[Product2[K, V]]._1
      dependencyID = externalSortDataElementTuple
      externalSortDataElementTuple.value._2
    }
  }

  /**
   * Insert the given iterator of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def insertAll(entries: Iterator[_ <: Any]): Unit = { // ver do que ele extende
    if (currentMap == null) {
      throw new IllegalStateException(
        "Cannot insert new elements into a map after calling iterator")
    }
    // An update function for the map that we reuse across entries to avoid allocating
    // a new closure each time
    var curEntry: EntriesMetaData[K,V] = null

    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, curEntry.value) else createCombiner(curEntry.value)
    }

    while (entries.hasNext) {
      curEntry = new EntriesMetaData(entries.next(), taskOfRDD)
      val estimatedSize = currentMap.estimateSize()
      if (estimatedSize > _peakMemoryUsedBytes) {
        _peakMemoryUsedBytes = estimatedSize
      }
      if (maybeSpill(currentMap, estimatedSize)) {
        currentMap = new SizeTrackingAppendOnlyMap[K, C](taskOfRDD)
      }
      currentMap.changeValue(curEntry.key, curEntry.key, curEntry.dependencyID, curEntry.alreadyExistsDataelement, update)
      addElementsRead()
    }
  }

  /**
   * Insert the given iterable of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def insertAll(entries: Iterable[DataElement[(K, V)]]): Unit = {
    insertAll(entries.iterator)
  }

  /**
   * Sort the existing contents of the in-memory map and spill them to a temporary file on disk.
   */
  override protected[this] def spill(collection: SizeTracker): Unit = {
    val inMemoryIterator = currentMap.destructiveSortedIterator(keyComparator)
    val diskMapIterator = spillMemoryIteratorToDisk(DataflowUtils.extractIteratorOfExternalAppendOnlyMap[K,C](inMemoryIterator))
    spilledMaps += diskMapIterator
  }

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  override protected[this] def forceSpill(): Boolean = {
    if (readingIterator != null) {
      val isSpilled = readingIterator.spill()
      if (isSpilled) {
        currentMap = null
      }
      isSpilled
    } else if (currentMap.size > 0) {
      spill(currentMap)
      currentMap = new SizeTrackingAppendOnlyMap[K, C](taskOfRDD)
      true
    } else {
      false
    }
  }

  /**
   * Spill the in-memory Iterator to a temporary file on disk.
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: Iterator[DataElement[(K, C)]])
      : DiskMapIterator = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    val writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, writeMetrics)
    var objectsWritten = 0

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables
    def flush(): Unit = {
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        val kv = inMemoryIterator.next()
        writer.write(kv.value._1, kv)
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      if (objectsWritten > 0) {
        flush()
        writer.close()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        writer.revertPartialWritesAndClose()
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    new DiskMapIterator(file, blockId, batchSizes)
  }

  /**
   * Returns a destructive iterator for iterating over the entries of this map.
   * If this iterator is forced spill to disk to release memory when there is not enough memory,
   * it returns pairs from an on-disk map.
   */
  def destructiveIterator(inMemoryIterator: Iterator[DataElement[(K, C)]]): Iterator[DataElement[(K, C)]] = {
    readingIterator = new SpillableIterator(inMemoryIterator)
    readingIterator.toCompletionIterator
  }

  /**
   * Return a destructive iterator that merges the in-memory map with the spilled maps.
   * If no spill has occurred, simply return the in-memory map's iterator.
   */
  override def iterator: Iterator[DataElement[(K, C)]] = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "ExternalAppendOnlyMap.iterator is destructive and should only be called once.")
    }
    if (spilledMaps.isEmpty) {
      destructiveIterator(currentMap.iterator)
    } else {
      new ExternalIterator()
    }
  }

  private def freeCurrentMap(): Unit = {
    if (currentMap != null) {
      currentMap = null // So that the memory can be garbage-collected
      releaseMemory()
    }
  }

  /**
   * An iterator that sort-merges (K, C) pairs from the in-memory map and the spilled maps
   */
  private class ExternalIterator extends Iterator[DataElement[(K, C)]] {

    // A queue that maintains a buffer for each stream we are currently merging
    // This queue maintains the invariant that it only contains non-empty buffers
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]

    // Input streams are derived both from the in-memory map and spilled maps on disk
    // The in-memory map is sorted in place, while the spilled maps are already in sorted order
    private val sortedMap = destructiveIterator(
      DataflowUtils.extractIteratorOfExternalAppendOnlyMap(currentMap.destructiveSortedIterator(keyComparator)))
    private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)

    inputStreams.foreach { it =>
      val kcPairs = new ArrayBuffer[DataElement[(K, C)]]
      readNextHashCode(it, kcPairs)
      if (kcPairs.length > 0) {
        mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
      }
    }

    /**
     * Fill a buffer with the next set of keys with the same hash code from a given iterator. We
     * read streams one hash code at a time to ensure we don't miss elements when they are merged.
     *
     * Assumes the given iterator is in sorted order of hash code.
     *
     * @param it iterator to read from
     * @param buf buffer to write the results into
     */
    private def readNextHashCode(it: BufferedIterator[DataElement[(K, C)]], buf: ArrayBuffer[DataElement[(K, C)]]): Unit = {
      if (it.hasNext) {
        var kc = it.next()
        buf += kc
        val minHash = hashKey(kc)
        while (it.hasNext && it.head.value._1.hashCode() == minHash) {
          kc = it.next()
          buf += kc
        }
      }
    }

    /**
     * If the given buffer contains a value for the given key, merge that value into
     * baseCombiner and remove the corresponding (K, C) pair from the buffer.
     */
    private def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): (C, DataElement[_ <: Any]) = {
      var i = 0
      while (i < buffer.pairs.length) {
        val pair = buffer.pairs(i)
        if (pair.value._1 == key) {
          // Note that there's at most one pair in the buffer with a given key, since we always
          // merge stuff in a map before spilling, so it's safe to return after the first we find
          removeFromBuffer(buffer.pairs, i)
          return (mergeCombiners(baseCombiner, pair.value._2), pair)
        }
        i += 1
      }
      (baseCombiner, null)
    }

    /**
     * Remove the index'th element from an ArrayBuffer in constant time, swapping another element
     * into its place. This is more efficient than the ArrayBuffer.remove method because it does
     * not have to shift all the elements in the array over. It works for our array buffers because
     * we don't care about the order of elements inside, we just want to search them for a key.
     */
    private def removeFromBuffer[T](buffer: ArrayBuffer[T], index: Int): T = {
      val elem = buffer(index)
      buffer(index) = buffer(buffer.size - 1)  // This also works if index == buffer.size - 1
      buffer.reduceToSize(buffer.size - 1)
      elem
    }

    /**
     * Return true if there exists an input stream that still has unvisited pairs.
     */
    override def hasNext: Boolean = mergeHeap.nonEmpty

    /**
     * Select a key with the minimum hash, then combine all values with the same key from all
     * input streams.
     */
    override def next(): DataElement[(K, C)] = {
      if (mergeHeap.isEmpty) {
        throw new NoSuchElementException
      }
      // Select a key from the StreamBuffer that holds the lowest key hash
      val minBuffer = mergeHeap.dequeue()
      val minPairs = minBuffer.pairs
      val minHash = minBuffer.minKeyHash
      val minPair = removeFromBuffer(minPairs, 0)
      val minKey = minPair.value._1
      var minCombiner = minPair.value._2
      assert(hashKey(minPair) == minHash)

      var dependenciesList = new mutable.MutableList[DataElement[_ <: Any]]()
      dependenciesList+=minPair

      // For all other streams that may have this key (i.e. have the same minimum key hash),
      // merge in the corresponding value (if any) from that stream
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      while (mergeHeap.nonEmpty && mergeHeap.head.minKeyHash == minHash) {
        val newBuffer = mergeHeap.dequeue()
        var resultMerge = mergeIfKeyExists(minKey, minCombiner, newBuffer)
        minCombiner = resultMerge._1
        if (resultMerge._2 != null)
          dependenciesList += resultMerge._2
        mergedBuffers += newBuffer
      }

      // Repopulate each visited stream buffer and add it back to the queue if it is non-empty
      mergedBuffers.foreach { buffer =>
        if (buffer.isEmpty) {
          readNextHashCode(buffer.iterator, buffer.pairs)
        }
        if (!buffer.isEmpty) {
          mergeHeap.enqueue(buffer)
        }
      }
      if (dependenciesList.size == 1) {
        minPair
      } else {
        val dependenciesListUUID = new java.util.ArrayList[UUID]()
        for (elem <- dependenciesList) {
          dependenciesListUUID.addAll(elem.dependenciesIDS)
          elem.deleteIt()
        }
        val result = DataElement.of((minKey, minCombiner), taskOfRDD, taskOfRDD.isIgnored, dependenciesListUUID)
        result
      }
    }

    /**
     * A buffer for streaming from a map iterator (in-memory or on-disk) sorted by key hash.
     * Each buffer maintains all of the key-value pairs with what is currently the lowest hash
     * code among keys in the stream. There may be multiple keys if there are hash collisions.
     * Note that because when we spill data out, we only spill one value for each key, there is
     * at most one element for each key.
     *
     * StreamBuffers are ordered by the minimum key hash currently available in their stream so
     * that we can put them into a heap and sort that.
     */
    private class StreamBuffer(
        val iterator: BufferedIterator[DataElement[(K, C)]],
        val pairs: ArrayBuffer[DataElement[(K, C)]])
      extends Comparable[StreamBuffer] {

      def isEmpty: Boolean = pairs.length == 0

      // Invalid if there are no more pairs in this stream
      def minKeyHash: Int = {
        assert(pairs.length > 0)
        hashKey(pairs.head)
      }

      override def compareTo(other: StreamBuffer): Int = {
        // descending order because mutable.PriorityQueue dequeues the max, not the min
        if (other.minKeyHash < minKeyHash) -1 else if (other.minKeyHash == minKeyHash) 0 else 1
      }
    }
  }

  /**
   * An iterator that returns (K, C) pairs in sorted order from an on-disk map
   */
  private class DiskMapIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[DataElement[(K, C)]]
  {
    private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1
    assert(file.length() == batchOffsets.last,
      "File length is not equal to the last batch offset:\n" +
      s"    file length = ${file.length}\n" +
      s"    last batch offset = ${batchOffsets.last}\n" +
      s"    all batch offsets = ${batchOffsets.mkString(",")}"
    )

    private var batchIndex = 0  // Which batch we're in
    private var fileStream: FileInputStream = null

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    private var deserializeStream: DeserializationStream = null
    private var nextItem: DataElement[(K, C)] = null
    private var objectsRead = 0

    /**
     * Construct a stream that reads only from the next batch.
     */
    private def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchIndex < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchIndex)
        fileStream = new FileInputStream(file)
        fileStream.getChannel.position(start)
        batchIndex += 1

        val end = batchOffsets(batchIndex)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val wrappedStream = serializerManager.wrapStream(blockId, bufferedStream)
        ser.deserializeStream(wrappedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    private def readNextItem(): DataElement[(K, C)] = {
      try {
        val k = deserializeStream.readKey().asInstanceOf[K]
        val c = deserializeStream.readValue().asInstanceOf[DataElement[(K, C)]]
        val item = (k, c)
        objectsRead += 1
        if (objectsRead == serializerBatchSize) {
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        item._2
      } catch {
        case e: EOFException =>
          cleanup()
          null
      }
    }

    override def hasNext: Boolean = {
      if (nextItem == null) {
        if (deserializeStream == null) {
          // In case of deserializeStream has not been initialized
          deserializeStream = nextBatchStream()
          if (deserializeStream == null) {
            return false
          }
        }
        nextItem = readNextItem()
      }
      nextItem != null
    }

    override def next(): DataElement[(K, C)] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val item = nextItem
      nextItem = null
      item
    }

    private def cleanup() {
      batchIndex = batchOffsets.length  // Prevent reading any other batch
      if (deserializeStream != null) {
        deserializeStream.close()
        deserializeStream = null
      }
      if (fileStream != null) {
        fileStream.close()
        fileStream = null
      }
      if (file.exists()) {
        if (!file.delete()) {
          logWarning(s"Error deleting ${file}")
        }
      }
    }

    context.addTaskCompletionListener[Unit](context => cleanup())
  }

  private class SpillableIterator(var upstream: Iterator[DataElement[(K, C)]])
    extends Iterator[DataElement[(K, C)]] {

    private val SPILL_LOCK = new Object()

    private var cur: DataElement[(K, C)] = readNext()

    private var hasSpilled: Boolean = false

    def spill(): Boolean = SPILL_LOCK.synchronized {
      if (hasSpilled) {
        false
      } else {
        logInfo(s"Task ${context.taskAttemptId} force spilling in-memory map to disk and " +
          s"it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")
        val nextUpstream = spillMemoryIteratorToDisk(upstream)
        assert(!upstream.hasNext)
        hasSpilled = true
        upstream = nextUpstream
        true
      }
    }

    private def destroy(): Unit = {
      freeCurrentMap()
      upstream = Iterator.empty
    }

    def toCompletionIterator: CompletionIterator[DataElement[(K, C)], SpillableIterator] = {
      CompletionIterator[DataElement[(K, C)], SpillableIterator](this, this.destroy)
    }

    def readNext(): DataElement[(K, C)] = SPILL_LOCK.synchronized {
      if (upstream.hasNext) {
        upstream.next()
      } else {
        null
      }
    }

    override def hasNext(): Boolean = cur != null

    override def next(): DataElement[(K, C)] = {
      val r = cur
      cur = readNext()
      r
    }
  }

  /** Convenience function to hash the given (K, C) pair by the key. */
  private def hashKey(kc: DataElement[(K, C)]): Int = ExternalAppendOnlyMap.hash(kc.value._1)

  override def toString(): String = {
    this.getClass.getName + "@" + java.lang.Integer.toHexString(this.hashCode())
  }
}

private[spark] object ExternalAppendOnlyMap {

  /**
   * Return the hash code of the given object. If the object is null, return a special hash code.
   */
  private def hash[T](obj: T): Int = {
    if (obj == null) 0 else obj.hashCode()
  }

  /**
   * A comparator which sorts arbitrary keys based on their hash codes.
   */
  private class HashComparator[K] extends Comparator[K] {
    def compare(key1: K, key2: K): Int = {
      val hash1 = hash(key1)
      val hash2 = hash(key2)
      if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
    }
  }
}
