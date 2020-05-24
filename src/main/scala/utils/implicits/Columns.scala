package utils.implicits

import java.sql.Timestamp
import java.text.{DecimalFormat, NumberFormat}
import java.util.Locale

import utils.Formatter.DateFormatters
import utils.Parameters.{CastMode, NOTPERMISSIVE, PERMISSIVE}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.functions.{col, struct, udf, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import utils.implicits.DataFrame._

import scala.util.{Failure, Success, Try}

object Columns {

  implicit class ColumnFunctions(column: Column) extends Serializable {

    /** Cast the child expression to the target data type.
      *
      * @param to   DataType
      * @param mode PERMISSIVE or NOTPERMISSIVE
      * @return Column casted
      */
    def cast(to: DataType, mode: CastMode): Column = mode match {
      case PERMISSIVE => column.cast(to)
      case NOTPERMISSIVE =>
        val colName      = column.toString
        val from         = column.expr.dataType
        val columnCasted = column.cast(to)
        val throwException = udf((value: String) => {
          //          throw new KirbyException(APPLY_FORMAT_CASTING_ERROR,
          //                                   s"$colName",
          //                                   s"$value",
          //                                   s"$from",
          //                                   s"$to")
          throw new Exception(
            s"13 - Apply Format Error: column $colName with value $value can not be casted from $from to $to")
          value
        })
        val isNotDataLost = !column.isNotNull.and(columnCasted.isNull)

        when(isNotDataLost, columnCasted).otherwise(throwException(column).cast(to))

    }

    /** Cast from stringType to the decimalType using a Locale.
      *
      * @param locale    locale to parse decimal if necessary. Default system locale
      * @param precision decimal precision
      * @param scale     decimal scale
      * @param mode      PERMISSIVE or NOTPERMISSIVE
      * @return Column casted
      */
    def parseDecimal(locale: Locale, precision: Int, scale: Int, mode: CastMode): Column = {
      val columnName      = column.toString()
      val numberFormatter = NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat]
      numberFormatter.setParseBigDecimal(true)
      val decimalParser = udf((number: String) =>
        Try(numberFormatter.parse(number).asInstanceOf[java.math.BigDecimal]) match {
          case Success(dec)                        => Some(dec)
          case Failure(_) if mode == PERMISSIVE    => None
          case Failure(e) if mode == NOTPERMISSIVE =>
            //            throw new KirbyException(APPLY_FORMAT_CASTING_ERROR,
            //                                     e,
            //                                     columnName,
            //                                     number,
            //                                     "StringType",
            //                                     s"DecimalType($precision, $scale)")
            throw new Exception(
              s"13 - Apply Format Error: column $columnName with value $number can not be casted from StringType to DecimalType($precision, $scale)"
            )
        })
      decimalParser(column).cast(DecimalType(precision, scale), mode)
    }

    /** Cast from stringType to the timestampType.
      *
      * @param format date pattern in java DateFormatter. Default yyyy-MM-dd HH:mm:ss
      * @param locale locale to parse date if necessary. Default system locale
      * @param mode   PERMISSIVE or NOTPERMISSIVE
      * @return Column casted
      */
    def parseTimestamp(format: Option[String], locale: Option[Locale], mode: CastMode): Column = {
      val columnName = column.toString()
      val dateParser = udf((date: String) => {
        Try(DateFormatters.parseTimestamp(date, format, locale)) match {
          case Success(d)                          => Some(d)
          case Failure(_) if mode == PERMISSIVE    => None
          case Failure(e) if mode == NOTPERMISSIVE =>
            //            throw new Exception(APPLY_FORMAT_CASTING_ERROR,
            //                                     e,
            //                                     columnName,
            //                                     date,
            //                                     "StringType",
            //                                     "TimestampType / DateType")
            throw new Exception(
              s"13 - Apply Format Error: column $columnName with value $date can " +
                s"not be casted from StringType to TimestampType / DateType")
        }
      })
      dateParser(column).cast(TimestampType, mode)
    }

    /** Cast from timestampType to stringType.
      *
      * @param format date pattern in java DateFormatter. Default yyyy-MM-dd HH:mm:ss
      * @param locale locale to parse date if necessary. Default system locale
      * @param mode   PERMISSIVE or NOTPERMISSIVE
      * @return Column casted
      */
    def formatTimestamp(format: Option[String], locale: Option[Locale], mode: CastMode): Column = {
      val columnName = column.toString()
      val dateFormatter = udf((date: Timestamp) => {
        Try(DateFormatters.formatTimestamp(date, format, locale)) match {
          case Success(d)                          => Some(d)
          case Failure(_) if mode == PERMISSIVE    => None
          case Failure(e) if mode == NOTPERMISSIVE =>
            //            throw new KirbyException(APPLY_FORMAT_CASTING_ERROR,
            //                                     e,
            //                                     columnName,
            //                                     date.toString,
            //                                     "TimestampType / DateType",
            //                                     "StringType")
            throw new Exception(
              s"13 - Apply Format Error: column $columnName with value ${date.toString} " +
                s"can not be casted from TimestampType / DateType to StringType")
        }
      })
      dateFormatter(column).cast(StringType, mode)
    }

    /** Reformat date from stringType to stringType the  data type.
      *
      * @param format   date pattern in java DateFormatter. Default yyyy-MM-dd HH:mm:ss
      * @param locale   locale to parse date if necessary. Default system locale
      * @param reformat date pattern in java DateFormatter. Default yyyy-MM-dd HH:mm:ss
      * @param relocale locale to parse date if necessary. Default system locale
      * @param mode     PERMISSIVE or NOTPERMISSIVE
      * @return Column casted
      */
    def reformatTimestamp(format: Option[String],
                          locale: Option[Locale],
                          reformat: Option[String],
                          relocale: Option[Locale],
                          mode: CastMode): Column = {
      val columnName = column.toString()
      val dateFormatter = udf((strDate: String) => {
        Try({
          val date = DateFormatters.parseTimestamp(strDate, format, locale)
          DateFormatters.formatTimestamp(date, reformat, relocale)
        }) match {
          case Success(d)                          => Some(d)
          case Failure(_) if mode == PERMISSIVE    => None
          case Failure(e) if mode == NOTPERMISSIVE =>
            //            throw new Exception(APPLY_FORMAT_CASTING_ERROR,
            //                                e,
            //                                columnName,
            //                                strDate,
            //                                "StringType",
            //                                "StringType")
            throw new Exception(
              s"13 - Apply Format Error: column $columnName with value $strDate " +
                s"can not be casted from StringType to StringType")
        }
      })
      dateFormatter(column).cast(StringType, mode)
    }

    /** Recover column metadata information
      *
      * @param column column to retrieve metadata
      * @return metadata associated
      */
    def getMetadata(): Metadata = column.expr match {
      case col: AttributeReference => col.metadata
      case _                       => Metadata.empty
    }

    def name(): String = column.expr match {
      case col: NamedExpression => col.name
      case _                    => throw new RuntimeException("Column does not have a name attribute.")
    }

    /**
      * Sets nullable attribute to true if possible.
      *
      * @return             Column with nodified nullabel attribute.
      */
    def nullableCol(): Column = {
      when(column.isNotNull, column)
    }

    /**
      * Create the nested structure in base of the names
      *
      * @param splitted   splitted name of the field which needs to be added
      * @return           Nested column
      */
    def createNestedStructs(splitted: Seq[String]): Column = {
      splitted.foldRight(column) {
        case (colName, nestedStruct) =>
          struct(nestedStruct as colName)
      }
    }

    /**
      * This recursive method takes a nested Field which needs to be added the new field inside the nested structure.
      * The resulting field will maintain the nullable attribute.
      *
      * @param parent       parent field name of the current field
      * @param splitted     splitted name of the field which needs to be added
      * @param df           DataFrame
      * @param colType      Field datatype
      * @param nullable     Flag if field is nullable
      * @param newCol       Field which needs to be added
      * @return             Field with added newCol
      */
    def recursiveAddNestedColumn(df: DataFrame,
                                 parent: String,
                                 splitted: Array[String],
                                 colType: StructType,
                                 nullable: Boolean,
                                 newCol: Column): Column = {

      /** field modification**/
      val modifiedFields: Seq[Column] =
        modifiedStructField(
          df,
          colType,
          splitted,
          parent,
          newCol
        )

      /** in order to maintain the nullable attribute**/
      if (nullable) {
        struct(modifiedFields: _*).nullableCol()
      } else {
        struct(modifiedFields: _*)
      }
    }

    /**
      * This recursive method takes a nested Field which needs to be added the new field inside the nested structure.
      *
      * @param df           DataFrame
      * @param colType      Field datatype
      * @param splitted     splitted name of the field which needs to be added
      * @param parent       parent field name of the current field
      * @param newCol       Field which needs to be added
      * @return             Field with added newCol
      */
    private def modifiedStructField(
                                     df: DataFrame,
                                     colType: StructType,
                                     splitted: Array[String],
                                     parent: String,
                                     newCol: Column
                                   ): Seq[Column] = {

      val sequence =
        if (parent == splitted.dropRight(1).mkString(".")) {
          Seq(newCol.nullableCol() as splitted.last)
        } else {
          Seq[Column]()
        }

      colType.fields
        .filter(f => s"$parent.${f.name}" != splitted.mkString("."))
        .foldRight(sequence) { (field, seq) =>
        {
          val curCol = recursiveDetection(
            df,
            field.dataType,
            field.name,
            field.nullable,
            parent,
            splitted,
            newCol
          )
          seq ++ Seq(curCol as field.name)
        }
        }
        .sortWith(_.name() < _.name())
    }

  }
}
