package transformation.transformations.column

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import transformation.errors.{ConversionError, TransformationError}
import transformation.transformations.Base64
import transformation.Transform._
import cats.implicits._
import transformation.{Parameters, Transform}
import transformation.Transform


/** Return column with default value.
  *
  * @param config config for literal masterization.
  */

object Base64Imp extends Parameters {
  object Base64Instance extends Base64Instance

  trait Base64Instance {
    implicit val Base64Transformation: Transform[Base64] =
      instance((op: Base64, col: Column) => transformation(op, col))
  }

  def transformation(Base64CC: Base64, col: Column): Either[TransformationError, Column] = {

    //    logger.info(s"Base64: The column  will be written in base64.")
    val _encrypted: Boolean = Base64CC.encrypted.setDefault(false, "encrypted")

    col.expr.dataType match {
      case StringType =>
        _encrypted match {
          case true  => unbase64(col).cast("string").asRight
          case false => base64(col).asRight
        }
      case _ =>
        ConversionError("Base64",
          s"The column ${Base64CC.field} cannot be casted to base64, " +
            s"since the data type is ${col.expr.dataType} and not Stringtype.").asLeft
    }
  }
}
