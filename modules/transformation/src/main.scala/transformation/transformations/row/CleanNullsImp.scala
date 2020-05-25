package transformation.transformations.row

import cats.implicits._
import transformation._
import transformation.errors.TransformationError
import transformation.Parameters
import transformation.transformations.CleanNulls
import org.apache.spark.sql.DataFrame
import transformation.{Parameters, Transform}

/** This class removes nulls from primary keys. If primary keys don't find, all columns will remove
  *
  * @param config transformation clean nulls.
  */
object CleanNullsImp extends Parameters {
  import Transform._
  object CleanNullsInstance extends CleanNullsInstance

  trait CleanNullsInstance {
    implicit val CleannullsTransformation: Transform[CleanNulls] = instance(
      (op: CleanNulls, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: CleanNulls, df: DataFrame): Either[TransformationError, DataFrame] = {

    val columnsToCheck: Seq[String] = op.primaryKey match {
      case Some(keys) => keys
      case _          => df.columns
    }

    logger.info("CleanNulls: checking clean nulls with following primary keys $columnsToCheck")

    df.na.drop(columnsToCheck).asRight
  }
}

//object Cleannulls {
//
//  def validated(
//      primaryKey: Option[Seq[String]] = None
//  ): Either[TransformationError, Cleannulls] = {
//    Right(Cleannulls(primaryKey))
//  }
//}
