package input

import com.typesafe.scalalogging.LazyLogging
import input.errors.InputError
import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless.{:+:, Coproduct, Inl, Inr}

trait Input[A] extends LazyLogging {
  def getInput(spark: SparkSession)(a: A): Either[InputError, DataFrame]
}

object Input extends GenericInstance with LazyLogging {
  def apply[A](implicit getInput: Input[A]): Input[A] = getInput

  def getInput[A: Input](spark: SparkSession)(a: A): Either[InputError, DataFrame] =
    Input[A].getInput(spark)(a)

  def instance[A](func: (A, SparkSession) => Either[InputError, DataFrame]): Input[A] =
    new Input[A] {
      def getInput(spark: SparkSession)(a: A): Either[InputError, DataFrame] = func(a, spark)
    }

  implicit class InputOps[A: Input](a: A) {
    def getInput(spark: SparkSession)(): Either[InputError, DataFrame] =
      Input[A].getInput(spark)(a)
  }
}



