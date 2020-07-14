package validation

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import validation.errors.ValidationError

trait Validate[A] extends LazyLogging {
  def testing(a: A)(df: DataFrame): Either[ValidationError, Boolean]
}

object Validate {
  def apply[A](implicit validate: Validate[A]): Validate[A] = validate

  def validateInstance[A: Validate](a: A)(
    df: DataFrame): Either[ValidationError, Boolean] =
    Validate[A].testing(a)(df)

  def validateInstance[A](
                           columnValidation: (A, DataFrame, String) => Either[ValidationError, Boolean]
                         ): Validate[A] = new Validate[A] {
    def testing(a: A)(df: DataFrame): Either[ValidationError, Boolean] =
      columnValidation(a, df.select(a.asInstanceOf[ColumnValidation].field), a.asInstanceOf[ColumnValidation].field)
  }

  //  def instance[A](
  //      rowValidation: (A, DataFrame) => Either[ValidationError, Boolean]
  //  ): Validate[A] =
  //    new Validate[A] {
  //      def testing(a: A)(df: DataFrame): Either[ValidationError, Boolean] =
  //        rowValidation(a, df)
  //    }

  implicit class ValidateOps[A: Validate](a: A) {
    def testing(df: DataFrame): Either[ValidationError, Boolean] =
      Validate[A].testing(a)(df)
  }
}