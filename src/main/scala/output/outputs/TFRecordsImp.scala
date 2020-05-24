package output.outputs

import output.Output._
import output.errors.OutputError
import org.apache.spark.sql.DataFrame
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging


object TFRecordsImp extends LazyLogging{

  object TFRecordsInstance extends TFRecordsInstance

  trait TFRecordsInstance {
    implicit val TFRecordsOutput: output.Output[TfRecord] =
      instance((op: TfRecord, df: DataFrame) => write(op, df))
  }

  def write(output: TfRecord, df: DataFrame): Either[OutputError, Unit] = {
    df.write.format("tfrecords").option("recordType", "Example").save(output.path).asRight
  }
}
