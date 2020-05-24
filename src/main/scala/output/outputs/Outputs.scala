package output.outputs

import org.apache.spark.sql.DataFrame
import output.errors.OutputError
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

sealed trait Output

case class TfRecord
(
  path: String
) extends Output


object OutputUtils extends OutputUtils

class OutputUtils {
  def getOutput[T<:Output](config: String): Either[ConfigReaderFailures, Output] = {
    implicit val hint = ProductHint[Output](useDefaultArgs = true)
    ConfigSource.string(config).load[Output]
  }
  def getGenericOutput[T](t: T)(df: DataFrame)(implicit outputs: output.Output[T]): Either[OutputError, Unit] =
    outputs.write(t)(df)
}


