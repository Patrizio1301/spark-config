package output

import cats.implicits._
import output.errors.{EmptyError, OutputError}
import org.apache.spark.sql.DataFrame
import shapeless.{:+:, CNil, Coproduct, Generic, Inl, Inr, Lazy}

trait GenericInstance {

  implicit val cnilInput: Output[CNil] =
    new Output[CNil] {
      override def write(t: CNil)(df: DataFrame): Either[OutputError, Unit] =
        EmptyError("", "").asLeft
    }

  implicit def coproductConsInput[L, R <: Coproduct](
      implicit
      lch: Output[L],
      rch: Output[R]): Output[L :+: R] =
    new Output[L :+: R] {
      override def write(t: L :+: R)(df: DataFrame): Either[OutputError, Unit] =
        t match {
          case Inl(l) => lch.write(l)(df)
          case Inr(r) => rch.write(r)(df)
        }
    }

  implicit def genericInput[A, G](implicit
                                      gen: Generic.Aux[A, G],
                                      cch: Lazy[Output[G]]): Output[A] =
    new Output[A] {
      def write(a: A)(df: DataFrame): Either[OutputError, Unit] =
        cch.value.write(gen.to(a))(df)
    }
}
