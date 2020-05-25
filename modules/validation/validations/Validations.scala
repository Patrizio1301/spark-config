package validation.validations

import validation.ColumnValidation
import utils.NumberLike.NumberLikeConverter
import utils.NumberLike.NumberLikeType._
import shapeless.CNil

import scala.runtime.Nothing$

sealed trait ValidationUnits

final case class RangeCase(
    field: String,
    min: NumberLikeType,
    max: NumberLikeType
) extends validation.ParamValidator[RangeCase]
    with ColumnValidation
    with ValidationUnits {
  def validation: Either[String, RangeCase] = ???
}

object RangeCase {
  def apply[A: NumberLikeConverter, B: NumberLikeConverter](
      field: String,
      min: A =null,
      max: B =null): RangeCase = {
    new RangeCase(field,
                     implicitly[NumberLikeConverter[A]].apply(min),
                     implicitly[NumberLikeConverter[B]].apply(max))
  }
}

final case class SelectColumns(
    columnsToSelect: Seq[String]
) extends validation.ParamValidator[SelectColumns]
    with ValidationUnits {
  def validation: Either[String, SelectColumns] = ???
}

object TransformationUtils extends TransformationUtils

class TransformationUtils {

//  def getTransformation[T <: ValidationUnits](
//      config: Config): Either[ConfigReaderFailures, ValidationUnits] = {
//    implicit val hint = ProductHint[ValidationUnits](useDefaultArgs = true)
//    implicit val hinte = ProductHint[ValidationUnits](allowUnknownKeys = false)
//    implicit def coproductHint[T] = new FieldCoproductHint[T]("type") {
//      override def fieldValue(name: String): String = name.toLowerCase
//    }
//    implicit def hinut[T] =
//      ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
//    import puregeneric.auto._
//    loadConfig[ValidationUnits](config)
//  }

}
