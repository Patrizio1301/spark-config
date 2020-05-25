package transformation.transformations.row

import transformation._
import transformation.errors.TransformationError
import cats.implicits._
import transformation.Parameters
import transformation.transformations.SqlFilter
import org.apache.spark.sql.DataFrame
import transformation.{Parameters, Transform}

object SqlFilterImp extends Parameters {
  import Transform._
  object SqlfilterInstance extends SqlfilterInstance

  trait SqlfilterInstance {
    implicit val SqlfilterTransformation: Transform[SqlFilter] = instance(
      (op: SqlFilter, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: SqlFilter, df: DataFrame): Either[TransformationError, DataFrame] = {
    df.filter(op.filter).asRight
  }
}
