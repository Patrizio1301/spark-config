package transformation.transformations.column

import cats.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import transformation.errors.TransformationError
import transformation.transformations.UpdateTime
import transformation.{Parameters, Transform}

/** This class create or replace a column with current time
  */
object UpdateTimeImp extends Parameters {
  import transformation.Transform._
  object UpdateTimeInstance extends UpdateTimeInstance

  trait UpdateTimeInstance {
    implicit val UpdateTimeTransformation: Transform[UpdateTime] =
      instance((op: UpdateTime, col: Column) => transformation(op, col))
  }

  /**
    * Method to transform column.
    *
    * @param col to be transformed.
    * @return Column transformed.
    */
  private def transformation(op: UpdateTime, col: Column): Either[TransformationError, Column] =
    current_timestamp().asRight
}
