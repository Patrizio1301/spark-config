package transformation.transformations.column

import cats.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import transformation.errors.TransformationError
import transformation.transformations.Hash
import transformation.transformations.Validator.domainValidation
import transformation.Transform
import utils.conversion.ConversionTypeUtil
import transformation.Transform._
import transformation.Transform
import transformation.{Parameters, Transform}

object HashImp extends Parameters with ConversionTypeUtil {

  object HashInstance extends HashInstance

  trait HashInstance {
    implicit val HashTransformation: Transform[Hash] =
      instance((op: Hash, col: Column) => transformation(op, col))
  }

  private def transformation(op: Hash, col: Column): Either[TransformationError, Column] = {
    val _hashLength: Int = op.hashLength.setDefault(256, "hashLength")
    val strCol           = col.cast(StringType)
    op.hashType.toLowerCase match {
      case "md5"  => md5(strCol).asRight
      case "sha1" => sha1(strCol).asRight
      case "sha2" => sha2(strCol, _hashLength).asRight
    }
  }

  def validated(
      field: String,
      hashType: String,
      hashLength: Option[Int] = None
  ): Either[TransformationError, Hash] = {

    for {
      validatedHashType <- domainValidation("Hash",
                                            "hashType",
                                            Seq("md5", "sha1", "sha2"),
                                            hashType)
    } yield
      new Hash(
        field,
        hashType,
        hashLength
      )
  }
}
