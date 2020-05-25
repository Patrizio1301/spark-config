package transformation.transformations.column

import utils.conversion.ConversionTypeUtil
import transformation.Parameters
import transformation.errors.TransformationError
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, SparkSession}
import cats.implicits._
import transformation.transformations.CopyColumn
import transformation.Transform._
import transformation.Parameters
import transformation.{Parameters, Transform}

/** Return column with value of other column
  */
object CopyColumnImp extends Parameters with ConversionTypeUtil {
  object CopyColumnInstance extends CopyColumnInstance

  trait CopyColumnInstance {
    implicit val CopyColumnTransformation: Transform[CopyColumn] =
      instance((op: CopyColumn, col: Column) => transformation(op, col))
  }

  def transformation(op: CopyColumn, col: Column): Either[TransformationError, Column] = {
    val spark = SparkSession.getDefaultSession.get
    lazy val _typeToCast: Option[DataType] = op.defaultType match {
      case Some(value) => typeToCast(value).toOption
      case _           => None
    }
    import spark.implicits._
    logger.info(
      s"CopyColumn: Copy value of column ${op.copyField} to column ${op.field} ${_typeToCast.map(
        "casting to " + _)}")

    val renameColumn = Symbol(op.copyField).as(op.field)
    _typeToCast.map(renameColumn.cast).getOrElse(renameColumn).asRight
  }
}
