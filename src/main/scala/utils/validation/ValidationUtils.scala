package utils.validation

import cats.Semigroup
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import cats.implicits._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object ValidationUtils {

  type ValidationSingle[Error, A]       = Validated[Error, A]
  type ValidationList[Error, A]         = Validated[List[Error], A]
  type ValidationNonEmptyList[Error, A] = Validated[NonEmptyList[Error], A]

  /**
    * Given two validated objects, this method compose both validations in the following manner:
    * A) If both validations are valid, the the function f applies to both valid values a and b.
    * B) If one of both validations is invalid, this error is reported as a output.
    * C) If both validations are invalid, both errors e1 and e2 are reported in a list List(e1,e2).
    *
    * @param v1   Validated object 1
    * @param v2   Validated object 2
    * @param f    Function for both valid outputs.
    * @return     Validated object with both validations composed.
    */
  def composeValidations[E: Semigroup, A, B, C](v1: Validated[E, A], v2: Validated[E, B])(
      f: (A, B) ⇒ C): Validated[E, C] =
    (v1, v2) match {
      case (Valid(a), Valid(b))       => Valid(f(a, b))
      case (i @ Invalid(_), Valid(_)) => i
      case (Valid(_), i @ Invalid(_)) => i
      case (Invalid(e1), Invalid(e2)) => Invalid(Semigroup[E].combine(e1, e2))
    }

  /**
    * Given a sequence of validations, this method composes those validations, in the following manner:
    * A) If all validations are valid, then the output is the struct type of sequence of fields.
    * B) If at least one validation is invalid, then all schema errors are reported in a list.
    *
    * @param fields             Sequence of ReadingResult[Seq[StructField]
    * @return                   ReadingResult type with a schema if the output is valid
    *                           and a schema error list if the output is invalid.
    */
  def extractFields[Error](
      fields: Seq[ValidationNonEmptyList[Error, Seq[StructField]]]
  ): ValidationNonEmptyList[Error, DataType] = {
    fields match {
      case _ if fields.nonEmpty =>
        fields.reduceLeft { (f1, f2) =>
          composeValidations(f1, f2)(_ ++ _)
        } match {
          case Valid(fieldSeq) => StructType(fieldSeq).valid
          case e @ Invalid(_)  => e
        }
      case _ => StructType(Seq()).valid
    }
  }
  /**
    * THIS FUNCTIONS MAY BE UTIL IN THE FUTURE.
    */
  /*def runValidations[S, T](
      obj: T,
      validations: (T => Validated[S, T])*
  ): ValidationList[T] = combine(obj, validations.toList.map(_.apply(obj)))*/

  /*
  /**
 *
 * @param obj
 * @param validations
 * @return
 */
  def combine[S, T](obj: T, validations: List[Validated[S, T]]): ValidationList[T] = {
    val errors = validations.collect { case Invalid(error) => error }
    if (errors.isEmpty) obj.valid else errors.invalid
  }*/

}
