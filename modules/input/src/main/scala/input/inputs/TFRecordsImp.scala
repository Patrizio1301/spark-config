package input.inputs

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import cats.implicits._
import input.Input
import input.errors.InputError
import input.Input._


object TFRecordsImp {

  object TFRecordInstance extends TFRecordInstance

  trait TFRecordInstance {
    implicit val TFRecordInput: Input[TKRecords] =
      instance((op: TKRecords,spark: SparkSession) => get(spark)(op))
  }

  def get(spark: SparkSession)(input: TKRecords): Either[InputError, DataFrame] = {

    //    logger.info(s"Base64: The column  will be written in base64.")
    spark.read.format("tfrecords").option("recordType", "Example").load(input.path).asRight
  }
}
