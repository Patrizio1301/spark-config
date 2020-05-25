package utils.implicits

import java.util.Locale

import utils.Parameters.{CastMode, NOTPERMISSIVE}
import utils.implicits.Columns._
import utils.implicits.DataFrame._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import utils.Formatter.LogicalFormatRegex._
import utils.SchemaFields.MetadataFields

import scala.util.matching.Regex
import scala.util.{Success, Try}

object ApplyFormat {

  def getFormat(field: StructField): Option[String] = {
    Try(field.metadata.getString(MetadataFields.FORMAT)) match {
      case Success(format) => Some(format)
      case _               => None
    }
  }

  def getLocale(field: StructField): Option[Locale] = {
    val localeMatcher: Regex = """([a-z,A-Z]*)[_-]([a-z,A-Z]*)""".r
    Try(field.metadata.getString(MetadataFields.LOCALE)) match {
      case Success(localeMatcher(lang, country)) =>
        Some(new Locale(lang, country))
      case Success(lang) =>
        Some(new Locale(lang))
      case _ => None
    }
  }
}

/** Apply format dataFrame util to cast columns types
  */
trait ApplyFormat extends LazyLogging {

  import ApplyFormat._

  val castMode: CastMode

  /** implicit class used to cast dataFrame column types
    *
    * @param df input dataFrame
    */
  implicit class ApplyFormatUtil(df: DataFrame) extends Serializable {

    /** Cast types of columns of input dataFrame to type of structType pass by parameter
      *
      * @param structType used to get types to cast dataFrame columns
      */
    def castSchemaFormat(structType: StructType, castMode: CastMode = castMode): DataFrame = {
      castDfToSchema(df, structType, castMode)
    }

    /** Cast types of columns of input dataFrame that are Date on 'logicalFormat' metadata
      *
      * @param structType used to get types to cast dataFrame columns
      */
    def castOriginTypeDate(structType: StructType, castMode: CastMode = castMode): DataFrame = {
      castDfToOriginTypeDate(df, structType, castMode)
    }

    /** Cast types of columns of input dataFrame that are Decimal on 'logicalFormat' metadata
      *
      * @param structType used to get types to cast dataFrame columns
      */
    def castOriginTypeDecimal(structType: StructType, castMode: CastMode = castMode): DataFrame = {
      castDfToOriginTypeDecimal(df, structType, castMode)
    }

    /** Cast types of columns of input dataFrame that are Timestamp on 'logicalFormat' metadata
      *
      * @param structType used to get types to cast dataFrame columns
      */
    def castOriginTypeTimestamp(structType: StructType,
                                castMode: CastMode = castMode): DataFrame = {
      castDfToOriginTypeTimestamp(df, structType, castMode)
    }

  }

  private def castDfToOriginTypeDecimal(df: DataFrame,
                                        schema: Seq[StructField],
                                        castMode: CastMode): DataFrame = {
    schema
      .foldLeft(df) { (df, field) =>
        Try(field.metadata.getString(MetadataFields.LOGICALFORMAT)) match {
          case Success(decimalPattern2(precision, scale, _, _)) =>
            val columnCasted =
              castColumnToDecimal(df, field, precision.toInt, scale.toInt, castMode)
            manageMalformed(df, columnCasted, field)
              .withNestedColumn(field.name, columnCasted.as(field.name, field.metadata))
          case Success(decimalPattern(precision, _, _)) =>
            df.withNestedColumn(field.name,
                                castColumnToDecimal(df, field, precision.toInt, 0, castMode)
                                  .as(field.name, field.metadata))
          case _ => df
        }
      }
  }

  private def castDfToOriginTypeTimestamp(df: DataFrame,
                                          schema: Seq[StructField],
                                          castMode: CastMode): DataFrame = {
    schema
      .foldLeft(df) { (df, field) =>
        Try(field.metadata.getString(MetadataFields.LOGICALFORMAT)) match {
          case Success(timestampPattern(_, _)) =>
            df.withNestedColumn(
              field.name,
              castColumnToTimestamp(df, field, castMode).as(field.name, field.metadata))
          case _ => df
        }
      }
  }

  private def castDfToOriginTypeDate(df: DataFrame,
                                     schema: Seq[StructField],
                                     castMode: CastMode): DataFrame = {
    schema
      .foldLeft(df) { (df, field) =>
        Try(field.metadata.getString(MetadataFields.LOGICALFORMAT)) match {
          case Success(datePattern(_, _)) =>
            val columnCasted = castColumnToDate(df, field, castMode).as(field.name, field.metadata)
            manageMalformed(df, columnCasted, field)
              .withNestedColumn(field.name, columnCasted.as(field.name, field.metadata))
          case _ => df
        }
      }
  }

  /**
    * Check if the datatype passed is simple or complex (if it has a nested structype or not)
    * @param valueType Dadatype being checked
    * @return true if it is simple, false in other case
    */
  private def isSimpleColumn(valueType: DataType): Boolean = {
    valueType match {
      case map: MapType     => isSimpleColumn(map.valueType)
      case array: ArrayType => isSimpleColumn(array.elementType)
      case _: StructType    => false
      case _: DataType      => true
    }
  }

  /**
    * Flattens the schema and transforms it in an array of columns, deleting nested structures
    * @param schema schema being flattened
    * @return an array of columns representing the new schema
    */
  private def flattenDataframe(schema: StructType): Array[Column] = {
    schema.fields.flatMap(f => {
      f.dataType match {
        case _: StructType                                          => None
        case map: MapType if !isSimpleColumn(map.valueType)         => None
        case array: ArrayType if !isSimpleColumn(array.elementType) => None
        case _                                                      => Array(col(f.name))
      }
    })
  }

  private def rowToCol(df: DataFrame): Column = {
    val a = df.select(flattenDataframe(df.schema): _*)
    concat_ws(",", a.columns.map(col): _*)
  }

  private def castDfToSchema(dfToApplyCast: DataFrame,
                             schema: Seq[StructField],
                             castMode: CastMode): DataFrame = {
    schema.foldLeft(dfToApplyCast) { (df, field) =>
      if (!hasColumnCorrectDataType(df.schema, field)) {
        val columnCasted = df(field.name).cast(field.dataType, castMode)

        val isDataLost = df(field.name).isNotNull && columnCasted.isNull
        val corruptedRecordsColumnUpdated =
          when(isDataLost || df("corruptRecords").isNotNull, rowToCol(df)).otherwise(null)
        df.withColumn("corruptRecords", corruptedRecordsColumnUpdated)
          .withNestedColumn(field.name, columnCasted.as(field.name, field.metadata))
      } else {
        df
      }
    }
  }

  private def hasColumnCorrectDataType(dfSchema: StructType, fieldToCast: StructField): Boolean = {
    dfSchema.fields.find(_.name == fieldToCast.name).forall(_.dataType == fieldToCast.dataType)
  }

  private def castColumnToTimestamp(df: DataFrame,
                                    field: StructField,
                                    castMode: CastMode): Column = {
    df.schema.filter(_.name == field.name).head.dataType match {
      case StringType =>
        val format = getFormat(field)
        val locale = getLocale(field)
        trim(df(field.name)).parseTimestamp(format, locale, castMode)
      case otherInvalid
          if !Cast.canCast(otherInvalid, TimestampType) && castMode == NOTPERMISSIVE =>
//        throw new KirbyException(APPLY_FORMAT_INVALID_CASTING,
//                                 field.name,
//                                 s"$otherInvalid",
//                                 "TimestampType / DateType")
        throw new Exception("HOLAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
      case _ =>
        df(field.name).cast(TimestampType, castMode)
    }
  }

  private def castColumnToDate(df: DataFrame, field: StructField, castMode: CastMode): Column = {
    to_date(castColumnToTimestamp(df, field, castMode))
  }

  /**
    * Writes a row in "corruptRecords" column if a column cast fails
    * @param df original dataframe
    * @param column column casted
    * @param field field casted
    * @return the original column with the correct data
    */
  private def manageMalformed(df: DataFrame, column: Column, field: StructField): DataFrame = {
    val isDataLost                    = df(field.name).isNotNull && column.isNull
    val corruptedRecordsColumnUpdated = when(isDataLost, rowToCol(df)).otherwise(null)
    df.withColumn("corruptRecords", corruptedRecordsColumnUpdated)
  }

  private def castColumnToDecimal(df: DataFrame,
                                  field: StructField,
                                  precision: Int,
                                  scale: Int,
                                  castMode: CastMode): Column = {
    logger.info(
      "ApplyFormatUtil: DecimalConversion -> Change the precision / scale in a given decimal to those set " +
        "in `decimalType` (if any), returning null if it overflows or modifying `value` in-place and returning it if successful.")
    df.schema.filter(_.name == field.name).head.dataType match {
      case StringType =>
        val fieldTrimmed = trim(df(field.name))
        Try(field.metadata.getString(MetadataFields.LOCALE)) match {
          case Success("fixed_signed_left") =>
            regexp_replace(fieldTrimmed, "^([+-])(\\d*?)(\\d{0," + scale + "}+)$", "$10$2\\.$3")
              .cast(DecimalType(precision, scale), castMode)
          case Success("fixed_signed_right") =>
            regexp_replace(fieldTrimmed, "^(\\d*?)(\\d{0," + scale + "}+)([+-])$", "$30$1\\.$2")
              .cast(DecimalType(precision, scale), castMode)
          case Success("fixed_unsigned") =>
            regexp_replace(fieldTrimmed, "^(\\d*?)(\\d{0," + scale + "}+)$", "0$1\\.$2")
              .cast(DecimalType(precision, scale), castMode)
          case _ =>
            getLocale(field) match {
              case Some(locale) => fieldTrimmed.parseDecimal(locale, precision, scale, castMode)
              case None         => fieldTrimmed.cast(DecimalType(precision, scale), castMode)
            }
        }
      case _ =>
        df(field.name).cast(DecimalType(precision, scale), castMode)
    }
  }

}
