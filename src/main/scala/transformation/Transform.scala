package transformation

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame}
import transformation.errors.TransformationError
import transformation.transformations.ColumnTransformation

import scala.util.{Failure, Success, Try}

trait Transform[A] extends LazyLogging {

  def transform(a: A)(b: DataFrame): Either[TransformationError, DataFrame]
}

object Transform extends GenericInstance with LazyLogging {
  def apply[A](implicit transform: Transform[A]): Transform[A] = transform

  def transform[A: Transform](a: A)(
    df: DataFrame): Either[TransformationError, DataFrame] =
    Transform[A].transform(a)(df)

  private def default[A] =
    (field: String, df: DataFrame, col: Column, op: A) =>
      transformDF(field, df, col, op)

  def instance[A](
                   columTfm: (A, Column) => Either[TransformationError, Column],
                   dfTfm: (String,
                     DataFrame,
                     Column,
                     A) => Either[TransformationError, DataFrame] = default
                 ): Transform[A] = new Transform[A] {
    def transform(a: A)(df: DataFrame): Either[TransformationError, DataFrame] =
      columTfm(a, getColumn(a.asInstanceOf[ColumnTransformation].field, df))
        .flatMap(col =>
          dfTfm(a.asInstanceOf[ColumnTransformation].field, df, col, a))
  }

  def instance[A](
                   func: (A, DataFrame) => Either[TransformationError, DataFrame])
  : Transform[A] =
    new Transform[A] {
      def transform(a: A)(
        df: DataFrame): Either[TransformationError, DataFrame] = func(a, df)
    }

  implicit class TransformOps[A: Transform](a: A) {
    def transform(df: DataFrame): Either[TransformationError, DataFrame] =
      Transform[A].transform(a)(df)
  }

  def getColumn(columnName: String, df: DataFrame): Column = {
    Try(df(columnName)) match {
      case Success(col) =>
        logger.debug(s"Transformer: Use existing column: $columnName")
        col
      case Failure(_) =>
        logger.debug(s"Transformer: Create new column $columnName")
        lit(None.orNull)
    }
  }

  def transformDF[A](fieldName: String,
                     df: DataFrame,
                     col: Column,
                     op: A): Either[TransformationError, DataFrame] = {
    df.withColumn(fieldName, col).asRight
  }
}
