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

package org.apache.spark.sql.catalyst.util

import java.text.ParseException
import java.time._
import java.time.format.DateTimeParseException
import java.time.temporal.TemporalQueries
import java.util.{Locale, TimeZone}

import scala.util.Try

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.internal.SQLConf

sealed trait TimestampFormatter extends Serializable {
  /**
   * Parses a timestamp in a string and converts it to microseconds.
   *
   * @param s - string with timestamp to parse
   * @return microseconds since epoch.
   * @throws ParseException can be thrown by legacy parser
   * @throws DateTimeParseException can be thrown by new parser
   * @throws DateTimeException unable to obtain local date or time
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  def parse(s: String): Long // returns microseconds since epoch
  def format(us: Long): String
}

class Iso8601TimestampFormatter(
    pattern: String,
    timeZone: TimeZone,
    locale: Locale) extends TimestampFormatter with DateTimeFormatterHelper {
  @transient
  private lazy val formatter = buildFormatter(pattern, locale)

  private def toInstant(s: String): Instant = {
    val temporalAccessor = formatter.parse(s)
    if (temporalAccessor.query(TemporalQueries.offset()) == null) {
      toInstantWithZoneId(temporalAccessor, timeZone.toZoneId)
    } else {
      Instant.from(temporalAccessor)
    }
  }

  private def instantToMicros(instant: Instant): Long = {
    val sec = Math.multiplyExact(instant.getEpochSecond, DateTimeUtils.MICROS_PER_SECOND)
    val result = Math.addExact(sec, instant.getNano / DateTimeUtils.NANOS_PER_MICROS)
    result
  }

  override def parse(s: String): Long = instantToMicros(toInstant(s))

  override def format(us: Long): String = {
    val secs = Math.floorDiv(us, DateTimeUtils.MICROS_PER_SECOND)
    val mos = Math.floorMod(us, DateTimeUtils.MICROS_PER_SECOND)
    val instant = Instant.ofEpochSecond(secs, mos * DateTimeUtils.NANOS_PER_MICROS)

    formatter.withZone(timeZone.toZoneId).format(instant)
  }
}

class LegacyTimestampFormatter(
    pattern: String,
    timeZone: TimeZone,
    locale: Locale) extends TimestampFormatter {
  @transient
  private lazy val format = FastDateFormat.getInstance(pattern, timeZone, locale)

  protected def toMillis(s: String): Long = format.parse(s).getTime

  override def parse(s: String): Long = toMillis(s) * DateTimeUtils.MICROS_PER_MILLIS

  override def format(us: Long): String = {
    format.format(DateTimeUtils.toJavaTimestamp(us))
  }
}

class LegacyFallbackTimestampFormatter(
    pattern: String,
    timeZone: TimeZone,
    locale: Locale) extends LegacyTimestampFormatter(pattern, timeZone, locale) {
  override def toMillis(s: String): Long = {
    Try {super.toMillis(s)}.getOrElse(DateTimeUtils.stringToTime(s).getTime)
  }
}

object TimestampFormatter {
  def apply(format: String, timeZone: TimeZone, locale: Locale): TimestampFormatter = {
    if (SQLConf.get.legacyTimeParserEnabled) {
      new LegacyFallbackTimestampFormatter(format, timeZone, locale)
    } else {
      new Iso8601TimestampFormatter(format, timeZone, locale)
    }
  }
}
