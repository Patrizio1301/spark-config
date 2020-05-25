package transformation.transformations.column

import transformation.ParamValidator
import transformation.errors.TransformationError
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import cats.implicits._
import transformation.transformations.CommaDelimiter
import transformation.Transform._
import transformation.Parameters
import transformation.{Parameters, Transform}

object CommaDelimiterImp extends Parameters {
  object CommaDelimiterInstance extends CommaDelimiterInstance

  trait CommaDelimiterInstance {
    implicit val CommaDelimiterTransformation: Transform[CommaDelimiter] =
      instance((op: CommaDelimiter, col: Column) => transformation(op, col))
  }

  /**
    * Custom trim transformation.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed, if applies.
    */
  def transformation(op: CommaDelimiter,
                             col: Column): Either[TransformationError, Column] = {
    val _lengthDecimal: Int       = op.lengthDecimal.setDefault(2, "lengthDecimal")
    val _separatorDecimal: String = op.separatorDecimal.setDefault(".", "separatorDecimal")
    val normalizedNumber =
      when(length(col) < _lengthDecimal + 1, lpad(col, _lengthDecimal + 1, "0")).otherwise(col)
    regexp_replace(normalizedNumber,
                   "^([+-]?)(\\d*?)(\\d{0," + _lengthDecimal + "}+)([+-]?)$",
                   "$1$4$2" + _separatorDecimal + "$3").asRight
  }
}
