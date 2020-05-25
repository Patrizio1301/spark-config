package input.inputs

import input.Input
import input.errors.InputError
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

sealed trait Inputs extends Product with Serializable

final case class Csv
(
  path: String
) extends Inputs


final case class TKRecords
(
  path: String
) extends Inputs

object InputUtils extends InputUtils

class InputUtils {
  def getInput[T<:Inputs](config: String): Either[ConfigReaderFailures, Inputs] = {
    implicit val hint = ProductHint[Inputs](useDefaultArgs = true)
    ConfigSource.string(config).load[Inputs]
  }

  def inputGeneric[T](spark: SparkSession)(t: T)(implicit swfT: Input[T]): Either[InputError, DataFrame] =
    swfT.getInput(spark)(t)
}

