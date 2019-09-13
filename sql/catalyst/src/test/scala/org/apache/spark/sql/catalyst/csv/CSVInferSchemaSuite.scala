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

package org.apache.spark.sql.catalyst.csv

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.types._

class CSVInferSchemaSuite extends SparkFunSuite with SQLHelper {

  test("String fields types are inferred correctly from null types") {
    val options = new CSVOptions(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"), false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(NullType, "") == NullType)
    assert(inferSchema.inferField(NullType, null) == NullType)
    assert(inferSchema.inferField(NullType, "100000000000") == LongType)
    assert(inferSchema.inferField(NullType, "60") == IntegerType)
    assert(inferSchema.inferField(NullType, "3.5") == DoubleType)
    assert(inferSchema.inferField(NullType, "test") == StringType)
    assert(inferSchema.inferField(NullType, "2015-08-20 15:57:00") == TimestampType)
    assert(inferSchema.inferField(NullType, "True") == BooleanType)
    assert(inferSchema.inferField(NullType, "FAlSE") == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(inferSchema.inferField(NullType, textValueOne) == expectedTypeOne)
  }

  test("String fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"), false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(LongType, "1.0") == DoubleType)
    assert(inferSchema.inferField(LongType, "test") == StringType)
    assert(inferSchema.inferField(IntegerType, "1.0") == DoubleType)
    assert(inferSchema.inferField(DoubleType, null) == DoubleType)
    assert(inferSchema.inferField(DoubleType, "test") == StringType)
    assert(inferSchema.inferField(LongType, "2015-08-20 14:57:00") == TimestampType)
    assert(inferSchema.inferField(DoubleType, "2015-08-20 15:57:00") == TimestampType)
    assert(inferSchema.inferField(LongType, "True") == BooleanType)
    assert(inferSchema.inferField(IntegerType, "FALSE") == BooleanType)
    assert(inferSchema.inferField(TimestampType, "FALSE") == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(inferSchema.inferField(IntegerType, textValueOne) == expectedTypeOne)
  }

  test("Timestamp field types are inferred correctly via custom data format") {
    var options = new CSVOptions(Map("timestampFormat" -> "yyyy-mm"), false, "GMT")
    var inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(TimestampType, "2015-08") == TimestampType)

    options = new CSVOptions(Map("timestampFormat" -> "yyyy"), false, "GMT")
    inferSchema = new CSVInferSchema(options)
    assert(inferSchema.inferField(TimestampType, "2015") == TimestampType)
  }

  test("Timestamp field types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(IntegerType, "2015-08-20 14") == StringType)
    assert(inferSchema.inferField(DoubleType, "2015-08-20 14:10") == StringType)
    assert(inferSchema.inferField(LongType, "2015-08 14:49:00") == StringType)
  }

  test("Boolean fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(LongType, "Fale") == StringType)
    assert(inferSchema.inferField(DoubleType, "TRUEe") == StringType)
  }

  test("Type arrays are merged to highest common type") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(
      inferSchema.mergeRowTypes(Array(StringType),
        Array(DoubleType)).deep == Array(StringType).deep)
    assert(
      inferSchema.mergeRowTypes(Array(IntegerType),
        Array(LongType)).deep == Array(LongType).deep)
    assert(
      inferSchema.mergeRowTypes(Array(DoubleType),
        Array(LongType)).deep == Array(DoubleType).deep)
  }

  test("Null fields are handled properly when a nullValue is specified") {
    var options = new CSVOptions(Map("nullValue" -> "null"), false, "GMT")
    var inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(NullType, "null") == NullType)
    assert(inferSchema.inferField(StringType, "null") == StringType)
    assert(inferSchema.inferField(LongType, "null") == LongType)

    options = new CSVOptions(Map("nullValue" -> "\\N"), false, "GMT")
    inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(IntegerType, "\\N") == IntegerType)
    assert(inferSchema.inferField(DoubleType, "\\N") == DoubleType)
    assert(inferSchema.inferField(TimestampType, "\\N") == TimestampType)
    assert(inferSchema.inferField(BooleanType, "\\N") == BooleanType)
    assert(inferSchema.inferField(DecimalType(1, 1), "\\N") == DecimalType(1, 1))
  }

  test("Merging Nulltypes should yield Nulltype.") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    val mergedNullTypes = inferSchema.mergeRowTypes(Array(NullType), Array(NullType))
    assert(mergedNullTypes.deep == Array(NullType).deep)
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val options = new CSVOptions(Map("TiMeStampFormat" -> "yyyy-mm"), false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(TimestampType, "2015-08") == TimestampType)
  }

  test("SPARK-18877: `inferField` on DecimalType should find a common type with `typeSoFar`") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    // 9.03E+12 is Decimal(3, -10) and 1.19E+11 is Decimal(3, -9).
    assert(inferSchema.inferField(DecimalType(3, -10), "1.19E11") ==
      DecimalType(4, -9))

    // BigDecimal("12345678901234567890.01234567890123456789") is precision 40 and scale 20.
    val value = "12345678901234567890.01234567890123456789"
    assert(inferSchema.inferField(DecimalType(3, -10), value) == DoubleType)

    // Seq(s"${Long.MaxValue}1", "2015-12-01 00:00:00") should be StringType
    assert(inferSchema.inferField(NullType, s"${Long.MaxValue}1") == DecimalType(20, 0))
    assert(inferSchema.inferField(DecimalType(20, 0), "2015-12-01 00:00:00")
      == StringType)
  }

  test("DoubleType should be inferred when user defined nan/inf are provided") {
    val options = new CSVOptions(Map("nanValue" -> "nan", "negativeInf" -> "-inf",
      "positiveInf" -> "inf"), false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(NullType, "nan") == DoubleType)
    assert(inferSchema.inferField(NullType, "inf") == DoubleType)
    assert(inferSchema.inferField(NullType, "-inf") == DoubleType)
  }

  test("inferring the decimal type using locale") {
    def checkDecimalInfer(langTag: String, expectedType: DataType): Unit = {
      val options = new CSVOptions(
        parameters = Map("locale" -> langTag, "inferSchema" -> "true", "sep" -> "|"),
        columnPruning = false,
        defaultTimeZoneId = "GMT")
      val inferSchema = new CSVInferSchema(options)

      val df = new DecimalFormat("", new DecimalFormatSymbols(Locale.forLanguageTag(langTag)))
      val input = df.format(Decimal(1000001).toBigDecimal)

      assert(inferSchema.inferField(NullType, input) == expectedType)
    }

    Seq("en-US", "ko-KR", "ru-RU", "de-DE").foreach(checkDecimalInfer(_, DecimalType(7, 0)))
  }
}
