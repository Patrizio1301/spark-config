package transformation

import transformation.errors.TransformationError
import org.apache.spark.sql.DataFrame
import shapeless.{:+:, CNil, Coproduct, Generic, Inl, Inr, Lazy}
import cats.implicits._

trait GenericInstance {

  implicit val cnilTransform: Transform[CNil] =
    new Transform[CNil] {
      override def transform(t: CNil)(
          df: DataFrame): Either[TransformationError, DataFrame] =
        df.asRight
    }

  implicit def coproductConsTransform[L, R <: Coproduct](
      implicit
      lch: Transform[L],
      rch: Transform[R]): Transform[L :+: R] =
    new Transform[L :+: R] {
      override def transform(t: L :+: R)(
          df: DataFrame): Either[TransformationError, DataFrame] =
        t match {
          case Inl(l) => lch.transform(l)(df)
          case Inr(r) => rch.transform(r)(df)
        }
    }

  implicit def genericTransform[A, G](implicit
                                      gen: Generic.Aux[A, G],
                                      cch: Lazy[Transform[G]]): Transform[A] =
    new Transform[A] {
      def transform(a: A)(
          df: DataFrame): Either[TransformationError, DataFrame] =
        cch.value.transform(gen.to(a))(df)
    }

}
