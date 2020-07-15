package scala.input.inputs

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import cats.implicits._
import input.Input
import input.errors.InputError
import input.Input._
import input.inputs.Excel


object ExcelImp {

  object ExcelInstance extends ExcelInstance

  trait ExcelInstance {
    implicit val ExcelInput: Input[Excel] =
      instance((op: Excel,spark: SparkSession) => get(spark)(op))
  }

  def get(spark: SparkSession)(input: Excel): Either[InputError, DataFrame] = {
    spark.read
      .format("com.crealytics.spark.excel")
      .option("location", input.path)
      .option("sheetName", input.sheetName) // Required
      .option("useHeader", input.useHeader) // Required
      .option("treatEmptyValuesAsNulls", input.treatEmptyValuesAsNulls) // Optional, default: true
      .option("inferSchema", input.inferSchema) // Optional, default: false
      .option("addColorColumns", input.addColorColumns) // Optional, default: false
      //.option("startColumn", 0) // Optional, default: 0
      //.option("endColumn", 99) // Optional, default: Int.MaxValue
      //.option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      //.option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      //.option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      .load(input.path).asRight
  }
}
