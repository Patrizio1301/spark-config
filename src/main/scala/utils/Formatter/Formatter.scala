package utils.Formatter

import java.sql.Timestamp
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.time.{LocalDateTime, ZoneId}
import java.util.Locale

import scala.collection.mutable

object DateFormatters {
  private lazy val formatters: mutable.Map[(Option[String], Option[Locale]), DateTimeFormatter] =
    mutable.HashMap()

  private def add(format: Option[String], locale: Option[Locale]): DateTimeFormatter = {
    val formatterBuilder = new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendPattern(format.getOrElse(""))
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .parseDefaulting(ChronoField.INSTANT_SECONDS, 0)
    val formatter = locale
      .map(locale => formatterBuilder.toFormatter(locale))
      .getOrElse(formatterBuilder.toFormatter)
    this.formatters.put((format, locale), formatter)
    formatter
  }

  def clear(): Unit = formatters.clear()

  def parseTimestamp(date: String, format: Option[String], locale: Option[Locale]): Timestamp = {
    val formatter: DateTimeFormatter = formatters.get((format, locale)) match {
      case None    => add(format, locale)
      case Some(f) => f
    }
    new Timestamp(
      LocalDateTime
        .from(formatter.parse(date))
        .atZone(ZoneId.systemDefault())
        .toInstant
        .toEpochMilli)
    Timestamp.valueOf(LocalDateTime.from(formatter.parse(date)))
  }

  def formatTimestamp(date: Timestamp, format: Option[String], locale: Option[Locale]): String = {
    val formatter: DateTimeFormatter = formatters.get((format, locale)) match {
      case None    => add(format, locale)
      case Some(f) => f
    }
    formatter.format(date.toLocalDateTime)
  }
}