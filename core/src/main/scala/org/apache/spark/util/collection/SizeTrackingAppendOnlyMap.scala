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

import br.uff.spark.{DataElement, Task}

/**
 * An append-only map that keeps track of its estimated size in bytes.
 */
private[spark] class SizeTrackingAppendOnlyMap[K, V](taskOfRDD: Task)
  extends AppendOnlyMap[K, V](taskOfRDD) with SizeTracker {

  override def update(key: K, value: DataElement[_ <: Any]): Unit = {
    super.update(key, value)
    super.afterUpdate()
  }

  override def changeValue(key: K, realKey: Any, dataElement: DataElement[_ <: Any], alreadyExistsDataelement: DataElement[_ <: Any], updateFunc: (Boolean, V) => V): V = {
    val newValue = super.changeValue(key, realKey, dataElement, alreadyExistsDataelement, updateFunc)
    super.afterUpdate()
    newValue
  }

  override protected def growTable(): Unit = {
    super.growTable()
    resetSamples()
  }
}
