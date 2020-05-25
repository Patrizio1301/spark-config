package transformation.transformations.column

import cats.data.Validated.{Invalid, Valid}
import cats.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.regexp_replace
import transformation.errors.{ConversionError, TransformationError}
import transformation.transformations.Formatter
import transformation.Transform
import utils.Parameters.PERMISSIVE
import utils.conversion.ConversionTypeUtil
import transformation.Transform._
import transformation.Parameters
import transformation.{Parameters, Transform}

object FormatterImp extends Parameters with ConversionTypeUtil {

  object FormatterInstance extends FormatterInstance

  trait FormatterInstance {
    implicit val FormatterTransformation: Transform[Formatter] =
      instance((op: Formatter, col: Column) => transformation(op, col))
  }

  /**
    * Custom formatter transformation.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed.
    */
  private def transformation(op: Formatter, col: Column): Either[TransformationError, Column] = {
    lazy val replacementsStr: String =
      op.replacements
        .map { r =>
          s"""pattern[${r.pattern}] replaced by [${r.replacement}]"""
        }
        .mkString(", ")

    logger.info("transform {} for column: {} to cast: {} with these replacements: {}",
                "Formatter",
                op.field,
                op.typeToCast,
                replacementsStr)

    val replace = op.replacements
      .foldLeft(col) { (column, replace) =>
        regexp_replace(column, replace.pattern, replace.replacement)
      }

    typeToCast(op.typeToCast) match {
      case Valid(tp) => replace.cast(tp).asRight
      case Invalid(error) =>
        ConversionError("Formatter", error.toList.map(_.toString()).mkString(" ")).asLeft
    }

  }
}
