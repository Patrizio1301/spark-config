package transformation.transformations.column

import transformation.Parameters
import transformation.errors.{InvalidValue, TransformationError}
import cats.implicits._
import transformation.transformations.Validator._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import transformation.transformations.CaseLetter
import transformation.Transform._
import transformation.Transform
import transformation.{Parameters, Transform}

/** Return column with default value.
  */
object CaseLetterImp extends Parameters {
  object CaseLetterInstance extends CaseLetterInstance

  trait CaseLetterInstance {
    implicit val CaseLetterTransformation: Transform[CaseLetter] =
      instance((op: CaseLetter, col: Column) => transformation(op, col))
  }

  def transformation(caseLetterCC: CaseLetter,
                             col: Column): Either[TransformationError, Column] = {

    logger.info(s"CaseLetter: ${caseLetterCC.operation} Case to column ${caseLetterCC.field}")

    caseLetterCC.operation match {
      case "lower" => lower(col).asRight
      case "upper" => upper(col).asRight
    }
  }

  def validated(
      field: String,
      operation: String
  ): Either[TransformationError, CaseLetter] = {

    for {
      validatedOperation <- domainValidation("CaseLetter",
                                             "operation",
                                             Seq("upper", "lower"),
                                             operation)
    } yield
      new CaseLetter(
        field,
        validatedOperation
      )
  }
}
