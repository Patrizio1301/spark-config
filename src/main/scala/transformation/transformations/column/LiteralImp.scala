package transformation.transformations.column

import cats.data.Validated.{Invalid, Valid}
import cats.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import transformation.errors.{InvalidValue, TransformationError}
import transformation.transformations.Literal
import transformation.{Parameters, Transform}
import utils.conversion.ConversionTypeUtil

/** Return column with default value.
  */
object LiteralImp extends Parameters with ConversionTypeUtil {
  import transformation.Transform._
  object LiteralInstance extends LiteralInstance

  trait LiteralInstance {
    implicit val LiteralTransformation: Transform[Literal] =
      instance((op: Literal, col: Column) => transformation(op, col))
  }

  private def transformation(op: Literal, col: Column): Either[TransformationError, Column] = {
    logger.info("Create literal column with default value : {} ", op.default)

    castIfNeeded(op.defaultType, lit(op.default))
  }

  private def castIfNeeded(defaultType: Option[String],
                           columnInString: Column): Either[TransformationError, Column] = {
    defaultType match {
      case Some(_defaultType) =>
        typeToCast(_defaultType) match {
          case Valid(__defaultType) => columnInString.cast(__defaultType).asRight
          case Invalid(e) =>
            InvalidValue("Literal",
                         "defaultType",
                         _defaultType,
                         e.map(_.toString()).mkString_(", ")).asLeft
        }
      case _ => columnInString.asRight
    }
  }
}
