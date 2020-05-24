package transformation.transformations.column

import cats.implicits._
import org.apache.spark.sql.{Column, DataFrame}
import transformation.errors.TransformationError
import transformation.transformations.Replace
import transformation.{Parameters, Transform}

/** Change field value for replace value if exist.
  * Because it's necessary use the DataFrame object to do the replace task, we have overridden the method
  * transform(DataFrame) to use the DataFrame directly, then we don't need to use the method transform(Column)
  */
/**
  * Transform to replace columns values using a map
  * @return DataFrame transformed.
  */
object ReplaceImp extends Parameters {
  import transformation.Transform._
  object ReplaceInstance extends ReplaceInstance

  trait ReplaceInstance {
    implicit val ReplaceTransformation: Transform[Replace] =
      instance((op: Replace, col: Column) => col.asRight,
               (field, df, col, op) => transformation(field, df, col, op))
  }

  /**
    * Transform to replace columns values using a map
    *
    * @param df to be transformed.
    * @return DataFrame transformed.
    */
  private def transformation(field: String,
                             df: DataFrame,
                             col: Column,
                             op: Replace): Either[TransformationError, DataFrame] = {
    logger.info(s"Replace: column $field with map: ${op.replace.mkString(", ")}")
    df.na.replace(field, op.replace).asRight
  }
}
