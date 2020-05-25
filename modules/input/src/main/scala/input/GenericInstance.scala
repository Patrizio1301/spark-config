package input

import input.errors.{EmptyError, InputError}
import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless.{:+:, CNil, Coproduct, Generic, Inl, Inr, Lazy}
import cats.implicits._
import input.inputs.CsvImp.CsvInstance
import input.inputs.TFRecordsImp.TFRecordInstance
import input.Input._

trait GenericInstance {

  implicit val cnilInput: Input[CNil] =
    new Input[CNil] {
      override def getInput(spark: SparkSession)(t: CNil): Either[InputError, DataFrame] =
        EmptyError("", "").asLeft
    }

  implicit def coproductConsInput[L, R <: Coproduct](
      implicit
      lch: Input[L],
      rch: Input[R]): Input[L :+: R] =
    new Input[L :+: R] {
      override def getInput(spark: SparkSession)(t: L :+: R): Either[InputError, DataFrame] =
        t match {
          case Inl(l) => lch.getInput(spark)(l)
          case Inr(r) => rch.getInput(spark)(r)
        }
    }

  implicit def genericInput[A, G](implicit
                                      gen: Generic.Aux[A, G],
                                      cch: Lazy[Input[G]]): Input[A] =
    new Input[A] {
      def getInput(spark: SparkSession)(a: A): Either[InputError, DataFrame] =
        cch.value.getInput(spark)(gen.to(a))
    }
}
