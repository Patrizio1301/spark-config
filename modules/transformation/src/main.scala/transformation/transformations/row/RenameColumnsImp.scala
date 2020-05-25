package transformation.transformations.row

import transformation._
import org.apache.spark.sql.DataFrame
import cats.implicits._
import utils.implicits.DataFrame._
import transformation.errors.TransformationError
import transformation.Parameters
import transformation.transformations.RenameColumns
import transformation.{Parameters, Transform}

/** This class rename columns by
  *
  * @param config rename columns.
  */
object RenameColumnsImp extends Parameters {
  import Transform._
  object RenamecolumnsInstance extends RenamecolumnsInstance

  trait RenamecolumnsInstance {
    implicit val RenamecolumnsTransformation: Transform[RenameColumns] = instance(
      (op: RenameColumns, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: RenameColumns, df: DataFrame): Either[TransformationError, DataFrame] = {
    logger.info(s"RenameColumns: rename columns: ${op.columnsToRename.mkString(",")}")
    op.columnsToRename.foldLeft(Right(df): Either[TransformationError, DataFrame]) {
      case (_df, rename) =>
        _df.map { __df =>
          __df.withNestedColumnRenamed(rename._1, rename._2)
        }
    }
  }
}
