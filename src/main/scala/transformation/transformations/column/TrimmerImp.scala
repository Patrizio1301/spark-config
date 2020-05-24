package transformation.transformations.column

import cats.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{ltrim, rtrim, trim, when}
import transformation.errors.{InvalidValue, TransformationError}
import transformation.transformations.Trimmer
import transformation.{Parameters, Transform}

/** Trims a string.
  */
object TrimmerImp extends Parameters {
  import transformation.Transform._
  object TrimmerInstance extends TrimmerInstance

  trait TrimmerInstance {
    implicit val TrimmerTransformation: Transform[Trimmer] =
      instance((op: Trimmer, col: Column) => transformation(op, col))
  }

  /**
    * Custom trim transformation.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed, if applies.
    */
  private def transformation(op: Trimmer, col: Column): Either[TransformationError, Column] = {
    val _trimType: String = op.trimType.setDefault("both", "trimType")
    when(
      col.isNotNull, {
        _trimType match {
          case "left"  => ltrim(col)
          case "right" => rtrim(col)
          case "both"  => trim(col)
        }
      }
    ).asRight
  }

  def validated(
      field: String,
      trimType: Option[String] = None
  ): Either[TransformationError, Trimmer] = {

    for {
      validatedTrimType <- validateTrimType(trimType)
    } yield
      new Trimmer(
        field,
        validatedTrimType
      )
  }

  private def validateTrimType(
      trimType: Option[String]
  ): Either[TransformationError, Option[String]] =
    Either.cond(
      trimType match {
        case Some(value) => Seq("left", "right", "both").contains(value.toLowerCase())
        case None        => true
      },
      trimType match {
        case Some(value) => Some(value.toLowerCase())
        case None        => None
      },
      InvalidValue(
        "Trimmer",
        "trimType",
        trimType.get.toLowerCase()
      )
    )
}
