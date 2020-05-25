package transformation.transformations.column

import cats.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import transformation.errors.TransformationError
import transformation.transformations.LeftPadding
import transformation.Parameters
import transformation.Transform
import transformation.{Parameters, Transform}

/** Cast a number to alphanumeric and preprend the fillCharacter until the length desired.
  * Parameters:
  *  - lengthDest (integer)
  *  - fillCharacter (string)
  *
  * The defaults are:
  * {
  * lengthDest = 4
  * fillCharacter = "0"
  * }
  */
object LeftPaddingImp extends Parameters {
  import transformation.Transform._
  object LeftPaddingInstance extends LeftPaddingInstance

  trait LeftPaddingInstance {
    implicit val LeftPaddingTransformation: Transform[LeftPadding] =
      instance((op, col) => colTransformation(op, col),
               (field, df, col, op) => dfTransformation(field, df, col, op))
  }

  /** Override to call fulfillment nulls after transform.
    *
    * @param df DataFrame to be transformed.
    * @return DataFrame transformed.
    */
  private def dfTransformation(field: String,
                               df: DataFrame,
                               col: Column,
                               op: LeftPadding): Either[TransformationError, DataFrame] = {
    val dfTransformed = transformDF(field, df, col, op)

    op.nullValue match {
      case Some(_nullValue) => dfTransformed.map(df => df.na.fill(Map(field -> _nullValue)))
      case _                => dfTransformed
    }
  }

  /** Call spark lpad function
    *
    * @param col to be transformed.
    * @return Column transformed.
    */
  private def colTransformation(op: LeftPadding,
                                col: Column): Either[TransformationError, Column] = {
    val _lengthDest: Int       = op.lengthDest.setDefault(4, "lengthDest")
    val _fillCharacter: String = op.fillCharacter.setDefault("0", "fillCharacter")
    lpad(col, _lengthDest, _fillCharacter).asRight
  }

}
