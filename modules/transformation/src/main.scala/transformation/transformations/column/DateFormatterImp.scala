package transformation.transformations.column

import java.util.Locale

import utils.Parameters.CastMode
import cats.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import transformation.errors.{InvalidValue, MissingValue, TransformationError}
import transformation.Parameters
import utils.conversion.ConversionTypeUtil
import transformation.CommonLiterals._
import transformation.transformations.DateFormatter
import utils.implicits.Columns._

import scala.util.matching.Regex
import scala.util.{Success, Try}
import transformation.Transform._
import transformation.Transform
import transformation.{Parameters, Transform}

/** Apply DateFormatter from String->Date/Timestamp, Date/Timestamp->String and String->String
  */
object DateFormatterImp extends Parameters with ConversionTypeUtil {
  object DateFormatterInstance extends DateFormatterInstance

  trait DateFormatterInstance {
    implicit val DateFormatterTransformation: Transform[DateFormatter] =
      instance((op: DateFormatter, col: Column) => transformation(op, col))
  }

  /**
    * Apply date format.
    *
    * @param col Column of interest to be transformed.
    * @return Column transformed.
    */
  private def transformation(op: DateFormatter,
                             col: Column): Either[TransformationError, Column] = {
    val _locale: String              = op.locale.setDefault("empty", "locale")
    val _relocale: String            = op.relocale.setDefault("empty", "relocale")
    val _castMode: String            = op.castMode.setDefault("permissive", "castMode")
    val _operation: String           = op.operation.setDefault("parse", "operation")
    lazy val localeToApply: Locale   = getLocale(_locale)
    lazy val reLocaleToApply: Locale = getLocale(_relocale)
    val castModeToApply: CastMode    = CastMode(_castMode)
    _operation match {
      case `formatOperation` =>
        logger.info(
          s"DateFormatter: apply format format operation with '${op.format}' to column ${op.field}")
        col.formatTimestamp(Some(op.format), Some(localeToApply), castModeToApply).asRight
      case `parseDateOperation` =>
        logger.info(
          s"DateFormatter: apply parse operation to date with '${op.format}' to column ${op.field}")
        to_date(col.parseTimestamp(Some(op.format), Some(localeToApply), castModeToApply)).asRight
      case `parseTimestampOperation` =>
        logger.info(
          s"DateFormatter: apply parse operation to timestamp with '${op.format}' to column ${op.field}")
        col.parseTimestamp(Some(op.format), Some(localeToApply), castModeToApply).asRight
      case `reformatOperation` =>
        logger.info(
          s"DateFormatter: apply reformat operation with '${op.format}'->'${op.reformat}' to column ${op.field}")
        col
          .reformatTimestamp(Some(op.format),
                             Some(localeToApply),
                             op.reformat,
                             Some(reLocaleToApply),
                             castModeToApply)
          .asRight
    }
  }

  private def getLocale(key: String) = {
    val localeMatcher: Regex = """([a-z,A-Z]*)[_-]([a-z,A-Z]*)""".r
    Try(key) match {
      case Success(localeMatcher(lang, country)) => new Locale(lang, country)
      case Success(lang)                         => new Locale(lang)
      case _                                     => new Locale("en")
    }
  }

  def validated(
      field: String,
      format: String,
      reformat: Option[String] = None,
      locale: Option[String] = None,
      relocale: Option[String] = None,
      castMode: Option[String] = None,
      operation: Option[String] = None
  ): Either[TransformationError, DateFormatter] = {

    for {
      validatedOperation <- validatedOperation(operation.getOrElse("parse"))
      validatedOperators <- validatedOperation2(validatedOperation, reformat)

    } yield
      new DateFormatter(
        field,
        format,
        reformat,
        locale,
        relocale,
        castMode,
        Some(validatedOperators)
      )
  }

  private val allowedOperations = Seq(
    reformatOperation,
    formatOperation,
    parseDateOperation,
    parseTimestampOperation
  )

  def validatedOperation(
      operation: String
  ): Either[TransformationError, String] =
    Either.cond(
      allowedOperations.contains(operation),
      operation,
      InvalidValue(
        "DateFormatter",
        "operation",
        operation,
        s"Available operations are ${allowedOperations.mkString(", ")}."
      )
    )

  def validatedOperation2(
      operation: String,
      reformat: Option[String] = None
  ): Either[TransformationError, String] =
    Either.cond(
      operation != reformatOperation || (operation == reformatOperation && reformat.isDefined),
      operation,
      MissingValue(
        "DateFormatter",
        "reformater"
      )
    )

}
