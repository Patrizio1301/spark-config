package transformation.transformations.row

import transformation._
import transformation.errors.{TransformationError, UnexpectedError}
import org.apache.spark.sql.DataFrame
import cats.implicits._
import transformation.Parameters
import transformation.transformations.DropColumns
import transformation.{Parameters, Transform}

import scala.util.{Failure, Success, Try}

/** This class removes columns by
  *
  * @param config drop columns.
  */
object DropColumnsImp extends Parameters {

  import Transform._

  object DropcolumnsInstance extends DropcolumnsInstance

  trait DropcolumnsInstance {
    implicit val DropcolumnsTransformation: Transform[DropColumns] = instance(
      (op: DropColumns, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: DropColumns, df: DataFrame): Either[TransformationError, DataFrame] = {
    import utils.implicits.DataFrame._
    Try(df.dropNestedColumn(op.columnsToDrop: _*)) match {
      case Success(dataframe) => dataframe.asRight
      case Failure(exception) => UnexpectedError("DropColumns", exception.getMessage).asLeft
    }
  }
}
