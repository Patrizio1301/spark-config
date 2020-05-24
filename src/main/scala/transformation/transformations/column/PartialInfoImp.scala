package transformation.transformations.column

import cats.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, SparkSession}
import transformation.errors.{InvalidValue, TransformationError}
import transformation.transformations.PartialInfo
import transformation.{Parameters, Transform}

/** Create new column with partial info of other column.
  */
object PartialInfoImp extends Parameters {
  import transformation.Transform._
  object PartialInfoInstance extends PartialInfoInstance

  trait PartialInfoInstance {
    implicit val PartialInfoTransformation: Transform[PartialInfo] =
      instance((op: PartialInfo, col: Column) => transformation(op, col))
  }

  private def transformation(op: PartialInfo, col: Column): Either[TransformationError, Column] = {

    val _start: Int  = op.start.setDefault(0, "start")
    val _length: Int = op.length.setDefault(0, "length")

    val spark = SparkSession.getDefaultSession.get

    import spark.implicits._

    logger.info(
      s"PartialInfo: substring from column ${op.fieldInfo}, start if ${_start} and lenght ${_length}")

    substring(Symbol(op.fieldInfo).cast(StringType), _start, _length).asRight
  }

  def validated(
      field: String,
      start: Option[Int] = None,
      length: Option[Int] = None,
      fieldInfo: String
  ): Either[TransformationError, PartialInfo] = {

    for {
      validatedStart  <- validateStart(start)
      validatedLength <- validateLength(length)
    } yield
      new PartialInfo(
        field,
        validatedStart,
        validatedLength,
        fieldInfo
      )
  }

  private def validateStart(
      start: Option[Int]
  ): Either[TransformationError, Option[Int]] =
    Either.cond(
      start match {
        case Some(value) => value >= 0
        case _           => true
      },
      start,
      InvalidValue(
        "PartialInfo",
        "start",
        s"${start.get}",
        "The value must be positive or zero."
      )
    )

  private def validateLength(
      length: Option[Int]
  ): Either[TransformationError, Option[Int]] =
    Either.cond(
      length match {
        case Some(value) => value >= 0
        case _           => true
      },
      length,
      InvalidValue(
        "PartialInfo",
        "start",
        s"${length.get}",
        "The value must be positive or zero."
      )
    )
}
