package transformation.transformations.column

import cats.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, SparkSession}
import transformation.errors.TransformationError
import transformation.transformations.ExtractInfoFromDate
import transformation.transformations.Validator.domainValidation
import transformation.Parameters
import transformation.Transform._
import transformation.Transform
import transformation.{Parameters, Transform}

/** Return column info from date of another column
  */
object ExtractInfoFromDateImp extends Parameters {

  object ExtractInfoFromDateInstance extends ExtractInfoFromDateInstance

  trait ExtractInfoFromDateInstance {
    implicit val ExtractInfoFromDateTransformation: Transform[ExtractInfoFromDate] =
      instance((op: ExtractInfoFromDate, col: Column) => transformation(op, col))
  }

  /** return year month or date from date column. To support backward compatibility we subtract one to the month.
    *
    * @param col to be transformed.
    * @return Column transformed.
    */
  private def transformation(op: ExtractInfoFromDate,
                             col: Column): Either[TransformationError, Column] = {
    val _dateField: String = op.dateField.toColumnName

    logger.info(s"ExtractInfoFromDate: Extract ${op.info} from ${_dateField} to ${op.field}")

    val spark = SparkSession.getDefaultSession.get

    import spark.implicits._

    (op.info match {
      case "day"   => dayofmonth(Symbol(_dateField))
      case "month" => month(Symbol(_dateField)).minus(lit(1))
      case "year"  => year(Symbol(_dateField))
    }).cast(StringType).asRight
  }

  def validated(
      field: String,
      dateField: String,
      info: String
  ): Either[TransformationError, ExtractInfoFromDate] = {

    for {
      validatedInfo <- domainValidation("ExtractInfoFromDate",
                                        "info",
                                        Seq("year", "month", "day"),
                                        info)
    } yield
      new ExtractInfoFromDate(
        field,
        dateField,
        validatedInfo
      )
  }
}
