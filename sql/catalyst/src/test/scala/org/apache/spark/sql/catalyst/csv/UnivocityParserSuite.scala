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

import java.math.BigDecimal
import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.{Locale, TimeZone}

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class UnivocityParserSuite extends SparkFunSuite with SQLHelper {
  private def assertNull(v: Any) = assert(v == null)

  test("Can parse decimal type values") {
    val stringValues = Seq("10.05", "1,000.01", "158,058,049.001")
    val decimalValues = Seq(10.05, 1000.01, 158058049.001)
    val decimalType = new DecimalType()

    stringValues.zip(decimalValues).foreach { case (strVal, decimalVal) =>
      val decimalValue = new BigDecimal(decimalVal.toString)
      val options = new CSVOptions(Map.empty[String, String], false, "GMT")
      val parser = new UnivocityParser(StructType(Seq.empty), options)
      assert(parser.makeConverter("_1", decimalType).apply(strVal) ===
        Decimal(decimalValue, decimalType.precision, decimalType.scale))
    }
  }

  test("Nullable types are handled") {
    val types = Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      BooleanType, DecimalType.DoubleDecimal, TimestampType, DateType, StringType)

    // Nullable field with nullValue option.
    types.foreach { t =>
      // Tests that a custom nullValue.
      val nullValueOptions = new CSVOptions(Map("nullValue" -> "-"), false, "GMT")
      var parser = new UnivocityParser(StructType(Seq.empty), nullValueOptions)
      val converter = parser.makeConverter("_1", t, nullable = true)
      assertNull(converter.apply("-"))
      assertNull(converter.apply(null))

      // Tests that the default nullValue is empty string.
      val options = new CSVOptions(Map.empty[String, String], false, "GMT")
      parser = new UnivocityParser(StructType(Seq.empty), options)
      assertNull(parser.makeConverter("_1", t, nullable = true).apply(""))
    }

    // Not nullable field with nullValue option.
    types.foreach { t =>
      // Casts a null to not nullable field should throw an exception.
      val options = new CSVOptions(Map("nullValue" -> "-"), false, "GMT")
      val parser = new UnivocityParser(StructType(Seq.empty), options)
      val converter = parser.makeConverter("_1", t, nullable = false)
      var message = intercept[RuntimeException] {
        converter.apply("-")
      }.getMessage
      assert(message.contains("null value found but field _1 is not nullable."))
      message = intercept[RuntimeException] {
        converter.apply(null)
      }.getMessage
      assert(message.contains("null value found but field _1 is not nullable."))
    }

    // If nullValue is different with empty string, then, empty string should not be casted into
    // null.
    Seq(true, false).foreach { b =>
      val options = new CSVOptions(Map("nullValue" -> "null"), false, "GMT")
      val parser = new UnivocityParser(StructType(Seq.empty), options)
      val converter = parser.makeConverter("_1", StringType, nullable = b)
      assert(converter.apply("") == UTF8String.fromString(""))
    }
  }

  test("Throws exception for empty string with non null type") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val parser = new UnivocityParser(StructType(Seq.empty), options)
    val exception = intercept[RuntimeException]{
      parser.makeConverter("_1", IntegerType, nullable = false).apply("")
    }
    assert(exception.getMessage.contains("null value found but field _1 is not nullable."))
  }

  test("Types are cast correctly") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    var parser = new UnivocityParser(StructType(Seq.empty), options)
    assert(parser.makeConverter("_1", ByteType).apply("10") == 10)
    assert(parser.makeConverter("_1", ShortType).apply("10") == 10)
    assert(parser.makeConverter("_1", IntegerType).apply("10") == 10)
    assert(parser.makeConverter("_1", LongType).apply("10") == 10)
    assert(parser.makeConverter("_1", FloatType).apply("1.00") == 1.0)
    assert(parser.makeConverter("_1", DoubleType).apply("1.00") == 1.0)
    assert(parser.makeConverter("_1", BooleanType).apply("true") == true)

    var timestampsOptions =
      new CSVOptions(Map("timestampFormat" -> "dd/MM/yyyy HH:mm"), false, "GMT")
    parser = new UnivocityParser(StructType(Seq.empty), timestampsOptions)
    val customTimestamp = "31/01/2015 00:00"
    var format = FastDateFormat.getInstance(
      timestampsOptions.timestampFormat, timestampsOptions.timeZone, timestampsOptions.locale)
    val expectedTime = format.parse(customTimestamp).getTime
    val castedTimestamp = parser.makeConverter("_1", TimestampType, nullable = true)
        .apply(customTimestamp)
    assert(castedTimestamp == expectedTime * 1000L)

    val customDate = "31/01/2015"
    val dateOptions = new CSVOptions(Map("dateFormat" -> "dd/MM/yyyy"), false, "GMT")
    parser = new UnivocityParser(StructType(Seq.empty), dateOptions)
    format = FastDateFormat.getInstance(
      dateOptions.dateFormat, dateOptions.timeZone, dateOptions.locale)
    val expectedDate = format.parse(customDate).getTime
    val castedDate = parser.makeConverter("_1", DateType, nullable = true)
        .apply(customDate)
    assert(castedDate == DateTimeUtils.millisToDays(expectedDate, TimeZone.getTimeZone("GMT")))

    val timestamp = "2015-01-01 00:00:00"
    timestampsOptions = new CSVOptions(Map(
      "timestampFormat" -> "yyyy-MM-dd HH:mm:ss",
      "dateFormat" -> "yyyy-MM-dd"), false, "UTC")
    parser = new UnivocityParser(StructType(Seq.empty), timestampsOptions)
    val expected = 1420070400 * DateTimeUtils.MICROS_PER_SECOND
    assert(parser.makeConverter("_1", TimestampType).apply(timestamp) ==
      expected)
    assert(parser.makeConverter("_1", DateType).apply("2015-01-01") ==
      expected / DateTimeUtils.MICROS_PER_DAY)
  }

  test("Throws exception for casting an invalid string to Float and Double Types") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val parser = new UnivocityParser(StructType(Seq.empty), options)
    val types = Seq(DoubleType, FloatType)
    val input = Seq("10u000", "abc", "1 2/3")
    types.foreach { dt =>
      input.foreach { v =>
        val message = intercept[NumberFormatException] {
          parser.makeConverter("_1", dt).apply(v)
        }.getMessage
        assert(message.contains(v))
      }
    }
  }

  test("Float NaN values are parsed correctly") {
    val options = new CSVOptions(Map("nanValue" -> "nn"), false, "GMT")
    val parser = new UnivocityParser(StructType(Seq.empty), options)
    val floatVal: Float = parser.makeConverter(
      "_1", FloatType, nullable = true).apply("nn").asInstanceOf[Float]

    // Java implements the IEEE-754 floating point standard which guarantees that any comparison
    // against NaN will return false (except != which returns true)
    assert(floatVal != floatVal)
  }

  test("Double NaN values are parsed correctly") {
    val options = new CSVOptions(Map("nanValue" -> "-"), false, "GMT")
    val parser = new UnivocityParser(StructType(Seq.empty), options)
    val doubleVal: Double = parser.makeConverter(
      "_1", DoubleType, nullable = true).apply("-").asInstanceOf[Double]

    assert(doubleVal.isNaN)
  }

  test("Float infinite values can be parsed") {
    val negativeInfOptions = new CSVOptions(Map("negativeInf" -> "max"), false, "GMT")
    var parser = new UnivocityParser(StructType(Seq.empty), negativeInfOptions)
    val floatVal1 = parser.makeConverter(
      "_1", FloatType, nullable = true).apply("max").asInstanceOf[Float]

    assert(floatVal1 == Float.NegativeInfinity)

    val positiveInfOptions = new CSVOptions(Map("positiveInf" -> "max"), false, "GMT")
    parser = new UnivocityParser(StructType(Seq.empty), positiveInfOptions)
    val floatVal2 = parser.makeConverter(
      "_1", FloatType, nullable = true).apply("max").asInstanceOf[Float]

    assert(floatVal2 == Float.PositiveInfinity)
  }

  test("Double infinite values can be parsed") {
    val negativeInfOptions = new CSVOptions(Map("negativeInf" -> "max"), false, "GMT")
    var parser = new UnivocityParser(StructType(Seq.empty), negativeInfOptions)
    val doubleVal1 = parser.makeConverter(
      "_1", DoubleType, nullable = true).apply("max").asInstanceOf[Double]

    assert(doubleVal1 == Double.NegativeInfinity)

    val positiveInfOptions = new CSVOptions(Map("positiveInf" -> "max"), false, "GMT")
    parser = new UnivocityParser(StructType(Seq.empty), positiveInfOptions)
    val doubleVal2 = parser.makeConverter(
      "_1", DoubleType, nullable = true).apply("max").asInstanceOf[Double]

    assert(doubleVal2 == Double.PositiveInfinity)
  }

  test("parse decimals using locale") {
    def checkDecimalParsing(langTag: String): Unit = {
      val decimalVal = new BigDecimal("1000.001")
      val decimalType = new DecimalType(10, 5)
      val expected = Decimal(decimalVal, decimalType.precision, decimalType.scale)
      val df = new DecimalFormat("", new DecimalFormatSymbols(Locale.forLanguageTag(langTag)))
      val input = df.format(expected.toBigDecimal)

      val options = new CSVOptions(Map("locale" -> langTag), false, "GMT")
      val parser = new UnivocityParser(new StructType().add("d", decimalType), options)

      assert(parser.makeConverter("_1", decimalType).apply(input) === expected)
    }

    Seq("en-US", "ko-KR", "ru-RU", "de-DE").foreach(checkDecimalParsing)
  }
}
