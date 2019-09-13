package br.uff.spark

import scala.collection.{AbstractIterator, Iterator}

/**
  * @author Thaylon Guedes Santos
  */
object DataflowUtils {

  def extractFromIterator[T](iter: Iterator[DataElement[T]]): Iterator[T] = new AbstractIterator[T]() {

    override def hasNext: Boolean = iter.hasNext

    override def next: T = iter.next.value

  }

  def extractRealIteratorOfAppendOnlyMap[K, C](original: Iterator[(K, DataElement[(Any, C)])]): Iterator[DataElement[(K, C)]] = new AbstractIterator[DataElement[(K, C)]] {
    override def hasNext: Boolean = original.hasNext

    override def next(): DataElement[(K, C)] = {
      val next = original.next()
      next._2.asInstanceOf[DataElement[(K, C)]]
    }
  }

  def extractIteratorOfExternalAppendOnlyMap[K, V](original: Iterator[(K, V)]): Iterator[DataElement[(K, V)]] = new AbstractIterator[DataElement[(K, V)]] {
    override def hasNext: Boolean = original.hasNext

    override def next(): DataElement[(K, V)] = {
      val next = original.next()
      next._2.asInstanceOf[DataElement[(K, V)]]
    }
  }

}