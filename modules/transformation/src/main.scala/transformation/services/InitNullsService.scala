package transformation.services

import cats.data.Validated.{Invalid, Valid}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{to_timestamp, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import utils.implicits.StructTypes._
import utils.implicits.DataFrame._
import utils.implicits.Columns._
import cats.implicits._
import utils.EitherUtils.EitherUtils._
import transformation.transformations.column.MaskImp.typeToCast
import transformation.errors.{InvalidValue, MissingValue, TransformationError, UnexpectedError}

import scala.util.{Failure, Success, Try}

/** Initialize nulls with default value.
  * Because it's necessary use the DataFrame object to do the init nulls task, we have overridden the method
  * transform(DataFrame) to use the DataFrame directly, then we don't need to use the method transform(Column)
  *
  * @param config config for init null masterization.
  */
object InitNullsService extends InitNullsService

/**
  * This trait is an overall service for InitNulls over one or several fields.
  */
trait InitNullsService extends LazyLogging {

  def apply(
      df: DataFrame,
      fields: Seq[String],
      default: Option[String],
      defaultType: Option[String] = None,
      outputSchema: StructType = StructType(Seq())): Either[TransformationError, DataFrame] = {

    val newColumns = sequence(
      fields.map(c => setDefaultValue(df, c, default, defaultType, outputSchema)))
    val columns = newColumns.map { columns =>
      (df.dropNestedColumn(getParents(fields): _*).columns.map(col) ++ columns).toSeq
    }

    columns.flatMap { selectedColumns =>
      applyNullablesToSchema(df.select(selectedColumns: _*), fields, outputSchema.unnested())
    }
  }

  def getParents(fields: Seq[String]): Seq[String] = {
    fields.map(_.split('.').head)
  }

  /**
    * columnWiseTransformation fullfills empty entries with default value for a given column
    *
    * @param df      DataFrame to be transformed.
    * @param field   field transformation is applied to.
    * @param default Default value for null entries.
    * @return DataFrame with null-entries in field fullfilled with default.
    */
  protected def setDefaultValue(
      df: DataFrame,
      field: String,
      default: Option[String],
      defaultType: Option[String] = None,
      outputSchema: StructType
  ): Either[TransformationError, Column] = {
    val col = Try(df(field)) match {
      case Success(col) =>
        logger.debug(s"InitNulls: Use existing column: $field")
        col
      case Failure(_) =>
        logger.debug(s"InitNulls: Create new column $field")
        lit(None.orNull)
    }

    getDefaultTypeByParameter(defaultType)

    val outputSchema_unnested = outputSchema.unnested()
    val defaultValue          = getDefaultValue(default, outputSchema_unnested, field)

    logger.info(s"InitNulls: column: $field with default value $defaultValue")
    defaultValue
      .flatMap { defaultValue =>
        col.expr.dataType match {
          case DateType =>
            when(col.isNull, to_date(lit(defaultValue))).otherwise(col).asRight
          case TimestampType =>
            when(col.isNull, to_timestamp(lit(defaultValue))).otherwise(col).asRight
          case NullType =>
            getDefaultTypeByParameter(defaultType).flatMap { dataType =>
              dataType match {
                case Some(value) =>
                  when(col.isNull, lit(defaultValue).cast(value)).otherwise(col).asRight
                case _ =>
                  getDefaultTypeByOuputSchema(outputSchema_unnested, field).map { dataType =>
                    when(col.isNull, lit(defaultValue).cast(dataType)).otherwise(col)
                  }
              }
            }
          case other: Any => when(col.isNull, lit(defaultValue).cast(other)).otherwise(col).asRight
        }
      }
      .map(col => createNestedCol(df, field, col))
  }

  def getDefaultValue(
      default: Option[String],
      outputSchema: StructType,
      field: String
  ): Either[TransformationError, String] = {
    default match {
      case Some(d) => d.asRight
      case None =>
        outputSchema.find(_.name == field) match {
          case Some(f) =>
            Try(f.metadata.getString("default")) match {
              case Success(d) => d.asRight
              case Failure(_) =>
                //TODO: Change error
                UnexpectedError(
                  "InitNulls",
                  "Schema field $field must contains default value or default attribute must be informed.").asLeft
            }
          //TODO: Change error
          case None => UnexpectedError("initNulls", s"schema field $field must exists.").asLeft
        }
    }
  }

  def getDefaultTypeByParameter(
      defaultType: Option[String]
  ): Either[TransformationError, Option[DataType]] = {
    defaultType match {
      case Some(_defaultType) =>
        typeToCast(_defaultType) match {
          case Valid(_dataType) => Some(_dataType).asRight
          case Invalid(e) =>
            InvalidValue("InitNulls",
                         "defaultType",
                         _defaultType,
                         e.toList.map(_.toString()).mkString(" ")).asLeft
        }
      case _ => None.asRight
    }
  }

  def getDefaultTypeByOuputSchema(
      outputSchema: StructType,
      field: String
  ): Either[TransformationError, DataType] =
    outputSchema.fields.find(_.name == field).map(_.dataType) match {
      case Some(_columnType) => _columnType.asRight
      case _                 => MissingValue("", field).asLeft
    }

  def applyNullablesToSchema(df: DataFrame,
                             fields: Seq[String],
                             outputSchema: StructType): Either[TransformationError, DataFrame] = {
    val newSchema = StructType(df.schema.unnested().map {
      case StructField(name, dataType, _, metadata) if fields.contains(name) =>
        StructField(name, dataType, nullable = outputSchema.contains(name) match {
          case true => outputSchema.find(_.name == name).get.nullable
          case _    => true
        }, metadata)
      case y: StructField => y
    })
    // apply new schema
    df.sqlContext.createDataFrame(df.rdd, newSchema.nested()).asRight
  }

  def createNestedCol(df: DataFrame, field: String, column: Column): Column = {
    if (field.contains('.')) {
      val splitted = field.split('.')
      val modifiedOrAdded: Column = df.schema.fields
        .find(_.name == splitted.head)
        .map(f => recursiveDetection(df, f.dataType, f.name, f.nullable, "", splitted, column))
        .getOrElse {
          column.createNestedStructs(splitted.tail) as splitted.head
        }
      modifiedOrAdded.name(splitted.head)
    } else {
      column.name(field)
    }
  }

}
