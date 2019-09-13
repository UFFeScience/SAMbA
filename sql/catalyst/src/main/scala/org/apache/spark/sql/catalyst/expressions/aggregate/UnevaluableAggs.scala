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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

abstract class UnevaluableBooleanAggBase(arg: Expression)
  extends UnevaluableAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = arg :: Nil

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = Seq(BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = {
    arg.dataType match {
      case dt if dt != BooleanType =>
        TypeCheckResult.TypeCheckFailure(s"Input to function '$prettyName' should have been " +
          s"${BooleanType.simpleString}, but it's [${arg.dataType.catalogString}].")
      case _ => TypeCheckResult.TypeCheckSuccess
    }
  }
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if all values of `expr` are true.",
  since = "3.0.0")
case class EveryAgg(arg: Expression) extends UnevaluableBooleanAggBase(arg) {
  override def nodeName: String = "Every"
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if at least one value of `expr` is true.",
  since = "3.0.0")
case class AnyAgg(arg: Expression) extends UnevaluableBooleanAggBase(arg) {
  override def nodeName: String = "Any"
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if at least one value of `expr` is true.",
  since = "3.0.0")
case class SomeAgg(arg: Expression) extends UnevaluableBooleanAggBase(arg) {
  override def nodeName: String = "Some"
}
