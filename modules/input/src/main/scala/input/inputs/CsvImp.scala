package input.inputs

import cats.implicits._
import input.Input
import input.errors.InputError
import org.apache.spark.sql.{DataFrame, SparkSession}
import input.Input._


object CsvImp {

  object CsvInstance extends CsvInstance

  trait CsvInstance {
    implicit val TFRecordsInput: Input[Csv] =
      instance((op: Csv, spark: SparkSession) => getCsv(spark)(op))
  }

  def getCsv(spark: SparkSession)(input: Csv): Either[InputError, DataFrame] = {

    //    logger.info(s"Base64: The column  will be written in base64.")
    spark.read.option("recordType", "Example").csv(input.path).asRight
  }
}
