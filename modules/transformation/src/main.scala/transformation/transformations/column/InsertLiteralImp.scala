package transformation.transformations.column

import cats.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import transformation.errors.{InvalidValue, TransformationError}
import transformation.transformations.InsertLiteral
import transformation.Parameters
import transformation.Transform._
import transformation.Transform
import transformation.{Parameters, Transform}

object InsertLiteralImp extends Parameters {

  object InsertLiteralInstance extends InsertLiteralInstance

  trait InsertLiteralInstance {
    implicit val InsertLiteralTransformation: Transform[InsertLiteral] =
      instance((op: InsertLiteral, col: Column) => transformation(op, col))
  }

  private def transformation(op: InsertLiteral,
                             col: Column): Either[TransformationError, Column] = {
    val _offsetFrom: String = op.offsetFrom.setDefault("left", "offsetFrom")
    val substrAdjustment    = 1

    _offsetFrom.toLowerCase match {
      case "left" => {
        val len = length(col)
        when(len.geq(op.offset),
             concat(col.substr(lit(0), lit(op.offset)),
                    lit(op.value),
                    col.substr(lit(op.offset + substrAdjustment), len - op.offset)))
          .otherwise(when(col.isNotNull, concat(col, lit(op.value))).otherwise(lit(op.value)))
          .asRight
      }
      case "right" => {
        val len = length(col)
        when(len.geq(op.offset),
             concat(col.substr(lit(0), len - op.offset),
                    lit(op.value),
                    col.substr(len - op.offset + substrAdjustment, lit(op.offset))))
          .otherwise(when(col.isNotNull, concat(lit(op.value), col)).otherwise(lit(op.value)))
          .asRight
      }
    }
  }

  def validated(
      field: String,
      offset: Int = 0,
      value: String,
      offsetFrom: Option[String] = None
  ): Either[TransformationError, InsertLiteral] = {

    for {
      validatedOffsetFrom <- validateOffsetFrom(offsetFrom)
    } yield
      new InsertLiteral(
        field,
        offset,
        value,
        validatedOffsetFrom
      )
  }

  private def validateOffsetFrom(
      offsetFrom: Option[String]
  ): Either[TransformationError, Option[String]] =
    Either.cond(
      offsetFrom match {
        case Some(value) => Seq("left", "right").contains(value.toLowerCase())
        case None        => true
      },
      offsetFrom match {
        case Some(value) => Some(value.toLowerCase())
        case None        => None
      },
      InvalidValue("InsertLiteral",
                   "offsetFrom",
                   offsetFrom.get,
                   "Only the values left and right are allowed for offsetFrom.")
    )

}
