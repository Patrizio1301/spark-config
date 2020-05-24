package transformation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{base64, unbase64}
import org.apache.spark.sql.types.StringType
import cats.implicits._
import transformation.errors.{ConversionError, TransformationError}

object Hello {
    def transformation(field: String, encrypted: Boolean, col: Column): Either[TransformationError, Column] = {

        //    logger.info(s"Base64: The column  will be written in base64.")

        col.expr.dataType match {
            case StringType =>
                encrypted match {
                    case true  => unbase64(col).cast("string").asRight
                    case false => base64(col).asRight
                }
            case _ =>
                ConversionError("Base64",
                    s"The column ${field} cannot be casted to base64, " +
                      s"since the data type is ${col.expr.dataType} and not Stringtype.").asLeft
        }
    }
}
