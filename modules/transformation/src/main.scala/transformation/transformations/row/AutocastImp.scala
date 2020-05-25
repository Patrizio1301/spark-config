package transformation.transformations.row

import cats.implicits._
import utils.Parameters.NOTPERMISSIVE
import utils.implicits.ApplyFormat
import utils.implicits.Columns._
import utils.implicits.DataFrame._
import utils.implicits.StructTypes._
import transformation._
import transformation.errors.{MissingValue, TransformationError, UnexpectedError}
import transformation.Transform
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}
import transformation.transformations.Autocast
import utils.conversion.ConversionTypeUtil
import transformation.{Parameters, Transform}

object AutocastImp extends Parameters with ConversionTypeUtil {

  import Transform._

  object AutocastInstance extends AutocastInstance

  trait AutocastInstance {
    implicit val AutoCastTransformation: Transform[Autocast] = instance(
      (op: Autocast, df: DataFrame) => transformation(op, df)
    )
  }

  private def transformation(
      op: Autocast,
      df: DataFrame
  ): Either[TransformationError, DataFrame] = {
    val unnestedSchema = df.schema.unnested()
    unnestedSchema.map(_.name).foldLeft(Right(df): Either[TransformationError, DataFrame]) {
      (seq, name) =>
        {
          unnestedSchema.find(_.name == name).map(_.dataType.simpleString) match {
            case Some(op.fromType) =>
              seq.flatMap(
                df =>
                  columnWiseTransformation(op, df, name)
                    .flatMap(column => seq.map(x => x.withNestedColumn(name, column))))
            case _ => seq
          }
        }
    }
  }

  private def columnWiseTransformation(
      op: Autocast,
      df: DataFrame,
      fieldName: String
  ): Either[TransformationError, Column] = {
    val col = df(fieldName)
    val field: Either[TransformationError, StructField] =
      df.schema
        .unnested()
        .find(_.name == fieldName) match {
        case Some(v) => v.asRight
        case _       => MissingValue("Autocast", s"Field $fieldName not found!").asLeft
      }
    if (op.exceptions.contains(fieldName)) {
      Right(col)
    } else {
      if (op.fromType == "date" && op.toType == "string") {
        field.map { field =>
          val format = ApplyFormat.getFormat(field)
          val locale = ApplyFormat.getLocale(field)
          col.formatTimestamp(format, locale, NOTPERMISSIVE)
        }
      } else if (op.fromType == "string" && op.toType == "date") {
        Right(to_date(col.parseTimestamp(op.format, None, NOTPERMISSIVE)))
      } else {
        typeToCast(op.toType)
          .map(validTypeToCast => col.cast(validTypeToCast, NOTPERMISSIVE))
          .toEither
          .leftMap { errors =>
            UnexpectedError("", errors.map(_.toString()).toList.mkString(" "))
          }
      }
    }
  }

  def validated(
      fromType: String,
      toType: String,
      format: Option[String] = None,
      exceptions: Seq[String] = Seq()
  ): Either[TransformationError, Autocast] = {

    for {
      validatedFromType <- validateFromType(fromType)
      validatedFormats  <- validateFormat(format, validatedFromType, toType)
    } yield
      Autocast(
        validatedFromType,
        toType,
        validatedFormats,
        exceptions
      )
  }

  private def validateFromType(fromType: String): Either[TransformationError, String] =
    Either.cond(
      fromType.nonEmpty,
      fromType,
      MissingValue("Autocast",
                   "There must be at least one operator to perform an operation." +
                     " Available operations are +, -, *, /, %, ^")
    )

  private def validateFormat(format: Option[String] = None,
                             fromType: String,
                             toType: String): Either[TransformationError, Option[String]] =
    Either.cond(
      format.nonEmpty && (fromType != "string" || toType != "date") && (fromType != "date" || toType != "string"),
      format,
      MissingValue("Autocast", "Required format configuration not found!")
    )

}
