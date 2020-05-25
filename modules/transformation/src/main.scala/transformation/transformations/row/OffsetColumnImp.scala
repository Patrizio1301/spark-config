package transformation.transformations.row

import transformation._
import transformation.errors.{InvalidValue, TransformationError}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import cats.implicits._
import transformation.Transform
import transformation.transformations.OffsetColumn
import transformation.{Parameters, Transform}

/** This class lag or leads a columns using windowing.
  * Mutable.Map used to preserver order.
  *
  * @param config offset column
  */
object OffsetColumnImp extends Parameters {
  import Transform._
  object OffsetColumnInstance extends OffsetColumnInstance

  trait OffsetColumnInstance {
    implicit val OffsetColumnTransformation: Transform[OffsetColumn] = instance(
      (op: OffsetColumn, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: OffsetColumn, df: DataFrame): Either[TransformationError, DataFrame] = {
    val window              = org.apache.spark.sql.expressions.Window.orderBy(op.columnToOrderBy)
    val _offset: Int        = op.offset.setDefault(1, "offset")
    val defaultName: String = s"${op.columnToLag}_${op.offsetType}${_offset}"
    val _newColumnName: String = op.newColumnName match {
      case Some(name) => name
      case _          => defaultName
    }
    import utils.implicits.DataFrame._
    op.offsetType.toLowerCase match {
      case "lag" =>
        df.withNestedColumn(_newColumnName, lag(op.columnToLag, _offset).over(window)).asRight
      case "lead" =>
        df.withNestedColumn(_newColumnName, lead(op.columnToLag, _offset).over(window)).asRight
      case _ =>
        InvalidValue("OffsetColumn",
                     "offsetType",
                     op.offsetType,
                     "offsetType must [lead] or [lag].").asLeft
    }
  }
}
