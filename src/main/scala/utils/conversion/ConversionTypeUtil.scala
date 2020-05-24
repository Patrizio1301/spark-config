package utils.conversion

import cats.data.NonEmptyList
import utils.validation.ValidationUtils.ValidationNonEmptyList
import utils.errors.{UtilError, ConversionError}
import org.apache.spark.sql.types._
import cats.implicits._

trait ConversionTypeUtil {

  /**
    * Hardcoded parsed file from different formats.
    *
    * @param dataType format from the schema file
    * @return Spark data type.
    */
  def typeToCast(dataType: String): ValidationNonEmptyList[UtilError, DataType] = {
    val decimalMatcher              = """^decimal *\( *(\d+) *, *(\d+) *\)$""".r
    val decimalOnlyPrecisionMatcher = """^decimal *\( *(\d+) *\)$""".r
    val compMatcher                 = """^comp *\( *(\d+) *\)$""".r
    val comp3Matcher                = """^comp-3 *\( *(\d+) *, *(\d+) *\)$""".r

    dataType.trim.toLowerCase match {
      case "string"                               => StringType.valid
      case "int32" | "int" | "integer"            => IntegerType.valid
      case "int64" | "long"                       => LongType.valid
      case "double"                               => DoubleType.valid
      case "float"                                => FloatType.valid
      case "boolean"                              => BooleanType.valid
      case "date"                                 => DateType.valid
      case "timestamp"                            => TimestampType.valid
      case "timestamp_millis"                     => TimestampType.valid
      case compMatcher(precision)                 => DecimalType(precision.toInt, 0).valid
      case "comp-1"                               => FloatType.valid
      case "comp-2"                               => DoubleType.valid
      case comp3Matcher(precision, scale)         => DecimalType(precision.toInt, scale.toInt).valid
      case decimalMatcher(precision, scale)       => DecimalType(precision.toInt, scale.toInt).valid
      case decimalOnlyPrecisionMatcher(precision) => DecimalType(precision.toInt, 0).valid
      case other: String                          => NonEmptyList(ConversionError(other, ""), List()).invalid
    }
  }

}
