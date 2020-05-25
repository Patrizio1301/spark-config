package output

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import output.errors.OutputError

trait Output[A] extends LazyLogging {
  def write(a: A)(df: DataFrame): Either[OutputError, Unit]
}

object Output extends GenericInstance with LazyLogging {
  def apply[A](implicit get: Output[A]): Output[A] = get

  def write[A: Output](a: A)(df: DataFrame): Either[OutputError, Unit] =
    Output[A].write(a)(df)

  def instance[A](func: (A, DataFrame) => Either[OutputError, Unit])
  : Output[A] =
    new Output[A] {
      def write(a: A)(df: DataFrame): Either[OutputError, Unit] = func(a, df)
    }

  implicit class OutputOps[A: Output](a: A) {
    def write(df: DataFrame): Either[OutputError, Unit] =
      Output[A].write(a)(df)
  }
}
