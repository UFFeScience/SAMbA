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

package org.apache.spark.sql.avro

import scala.util.control.NonFatal

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, SpecificInternalRow, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.types._

case class AvroDataToCatalyst(
    child: Expression,
    jsonFormatSchema: String,
    options: Map[String, String])
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val dataType: DataType = {
    val dt = SchemaConverters.toSqlType(avroSchema).dataType
    parseMode match {
      // With PermissiveMode, the output Catalyst row might contain columns of null values for
      // corrupt records, even if some of the columns are not nullable in the user-provided schema.
      // Therefore we force the schema to be all nullable here.
      case PermissiveMode => dt.asNullable
      case _ => dt
    }
  }

  override def nullable: Boolean = true

  @transient private lazy val avroSchema = new Schema.Parser().parse(jsonFormatSchema)

  @transient private lazy val reader = new GenericDatumReader[Any](avroSchema)

  @transient private lazy val deserializer = new AvroDeserializer(avroSchema, dataType)

  @transient private var decoder: BinaryDecoder = _

  @transient private var result: Any = _

  @transient private lazy val parseMode: ParseMode = {
    val mode = AvroOptions(options).parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw new AnalysisException(unacceptableModeMessage(mode.name))
    }
    mode
  }

  private def unacceptableModeMessage(name: String): String = {
    s"from_avro() doesn't support the $name mode. " +
      s"Acceptable modes are ${PermissiveMode.name} and ${FailFastMode.name}."
  }

  @transient private lazy val nullResultRow: Any = dataType match {
      case st: StructType =>
        val resultRow = new SpecificInternalRow(st.map(_.dataType))
        for(i <- 0 until st.length) {
          resultRow.setNullAt(i)
        }
        resultRow

      case _ =>
        null
    }


  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    try {
      decoder = DecoderFactory.get().binaryDecoder(binary, 0, binary.length, decoder)
      result = reader.read(result, decoder)
      deserializer.deserialize(result)
    } catch {
      // There could be multiple possible exceptions here, e.g. java.io.IOException,
      // AvroRuntimeException, ArrayIndexOutOfBoundsException, etc.
      // To make it simple, catch all the exceptions here.
      case NonFatal(e) => parseMode match {
        case PermissiveMode => nullResultRow
        case FailFastMode =>
          throw new SparkException("Malformed records are detected in record parsing. " +
            s"Current parse Mode: ${FailFastMode.name}. To process malformed records as null " +
            "result, try setting the option 'mode' as 'PERMISSIVE'.", e)
        case _ =>
          throw new AnalysisException(unacceptableModeMessage(parseMode.name))
      }
    }
  }

  override def prettyName: String = "from_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, eval => {
      val result = ctx.freshName("result")
      val dt = CodeGenerator.boxedType(dataType)
      s"""
        $dt $result = ($dt) $expr.nullSafeEval($eval);
        if ($result == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = $result;
        }
      """
    })
  }
}
