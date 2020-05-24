package utils.NumberLike

import shapeless.{:+:, CNil, Coproduct, Generic, Inl, Inr, Lazy}
import java.sql.Timestamp
import cats.implicits._
import utils.errors.ConversionError

import utils.errors.UtilError

trait NumberLike[A] {
  type B
  def lessThanOrEqual(a: A, b: B): Either[UtilError, Boolean]
  def moreThanOrEqual(a: A, b: B): Either[UtilError, Boolean]
}

object NumberLike {
  type Aux[A, B0] = NumberLike[A] { type B = B0 }

  def apply[A, B](implicit numberLike: Aux[A, B]): Aux[A, B] =
    numberLike

  def lessThenOrEqual[A, B](a: A)(b: B)(
      implicit numberLike: Aux[A, B]): Either[UtilError, Boolean] =
    numberLike.lessThanOrEqual(a, b)
  def moreThenOrEqual[A, B](a: A)(b: B)(
      implicit numberLike: Aux[A, B]): Either[UtilError, Boolean] =
    numberLike.moreThanOrEqual(a, b)

  def instance[A, B0](
      lTOE: (A, B0) => Either[UtilError, Boolean],
      mTOE: (A, B0) => Either[UtilError, Boolean]
  ): Aux[A, B0] = new NumberLike[A] {
    type B = B0
    def lessThanOrEqual(a: A, b: B): Either[UtilError, Boolean] = lTOE(a, b)
    def moreThanOrEqual(a: A, b: B): Either[UtilError, Boolean] = mTOE(a, b)
  }

  object ops {
    implicit class NumberLikeOps[A](a: A) {
      def lessThanOrEqual[B](b: B)(
          implicit numberLike: Aux[A, B]): Either[UtilError, Boolean] =
        numberLike.lessThanOrEqual(a, b)
      def moreThanOrEqual[B](b: B)(
          implicit numberLike: Aux[A, B]): Either[UtilError, Boolean] =
        numberLike.moreThanOrEqual(a, b)
    }
  }

  val le = (a: Int, b: Int) => a <= b

  implicit val intIntNumberLike: Aux[Int, Int] =
    instance((a, b) => (a <= b).asRight, (a, b) => (a >= b).asRight)
  implicit val intFloatNumberLike: Aux[Int, Float] =
    instance((a, b) => (a <= b).asRight, (a, b) => (a >= b).asRight)
  implicit val intDoubleNumberLike: Aux[Int, Double] =
    instance((a, b) => (a <= b).asRight, (a, b) => (a >= b).asRight)
  implicit val intNullNumberLike: Aux[Int, Null] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val DoubleIntNumberLike: Aux[Double, Int] =
    instance((a, b) => (a <= b).asRight, (a, b) => (a >= b).asRight)
  implicit val DoubleFloatNumberLike: Aux[Double, Float] =
    instance((a, b) => (a <= b).asRight, (a, b) => (a >= b).asRight)
  implicit val DoubleDoubleNumberLike: Aux[Double, Double] =
    instance((a, b) => (a <= b).asRight, (a, b) => (a >= b).asRight)
  implicit val DoubleNullNumberLike: Aux[Double, Null] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val floatIntNumberLike: Aux[Float, Int] =
    instance((a, b) => (a <= b).asRight, (a, b) => (a >= b).asRight)
  implicit val floatFloatNumberLike: Aux[Float, Float] =
    instance((a, b) => (a <= b).asRight, (a, b) => (a >= b).asRight)
  implicit val floatDoubleNumberLike: Aux[Float, Double] =
    instance((a, b) => (a <= b).asRight, (a, b) => (a >= b).asRight)
  implicit val floatNullNumberLike: Aux[Float, Null] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val nullIntNumberLike: Aux[Null, Int] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val nullFloatNumberLike: Aux[Null, Float] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val nullDoubleNumberLike: Aux[Null, Double] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val nullNullNumberLike: Aux[Null, Null] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)

  implicit val cnilNumberLike: Aux[CNil, CNil] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val intCnilNumberLike: Aux[Int, CNil] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val floatCnilNumberLike: Aux[Float, CNil] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val doubleCnilNumberLike: Aux[Double, CNil] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val nullCnilNumberLike: Aux[Null, CNil] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val cnilIntNumberLike: Aux[CNil, Int] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val cnilFloatNumberLike: Aux[CNil, Float] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val cnilDoubleNumberLike: Aux[CNil, Double] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val cnilNullNumberLike: Aux[CNil, Null] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)

  implicit val timestampTimestampNumberLike: Aux[Timestamp, Timestamp] =
    instance((a, b) => a.before(b).asRight, (a, b) => a.after(b).asRight)
  implicit val timestampNullNumberLike: Aux[Timestamp, Null] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val nullTimestampNumberLike: Aux[Null, Timestamp] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)

  implicit val cnilTimestampNumberLike: Aux[CNil, Timestamp] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)
  implicit val timestampCNilNumberLike: Aux[Timestamp, CNil] =
    instance((_, _) => true.asRight, (_, _) => true.asRight)

  implicit def coproductTransform[L, R <: Coproduct, LL, RR <: Coproduct](
      implicit
      lch: Aux[L, LL],
      lch2: Aux[L, RR],
      rch: Aux[R, LL],
      rch2: Aux[R, RR]): Aux[L :+: R, LL :+: RR] =
    instance(
      {
        case (Inl(l), Inl(ll)) => lch.lessThanOrEqual(l, ll)
        case (Inl(l), Inr(rr)) => lch2.lessThanOrEqual(l, rr)
        case (Inr(r), Inl(ll)) => rch.lessThanOrEqual(r, ll)
        case (Inr(r), Inr(rr)) => rch2.lessThanOrEqual(r, rr)
      }, {
        case (Inl(l), Inl(ll)) => lch.moreThanOrEqual(l, ll)
        case (Inl(l), Inr(rr)) => lch2.moreThanOrEqual(l, rr)
        case (Inr(r), Inl(ll)) => rch.moreThanOrEqual(r, ll)
        case (Inr(r), Inr(rr)) => rch2.moreThanOrEqual(r, rr)
      }
    )

  implicit def coproductTransform2[L, R <: Coproduct, LL <:Coproduct](
      implicit
      lch: Aux[L, Any],
      lch2: Aux[R, Any]): Aux[L :+: R, LL] =
    instance(
      {
        case (Inl(l), Inl(ll)) => lch.lessThanOrEqual(l, ll)
        case (Inr(r), Inl(ll)) => lch2.lessThanOrEqual(r, ll)
      }, {
        case (Inl(l), Inl(ll)) => lch.moreThanOrEqual(l, ll)
        case (Inr(r), Inl(rr)) => lch2.moreThanOrEqual(r, rr)
      }
    )

  implicit def coproductCNilTransform[L, R <: Coproduct, CNil](
      implicit
      right: Aux[R, CNil],
      left: Aux[L, CNil]
  ): Aux[L :+: R, CNil] =
    instance(
      {
        case (Inl(l), x: CNil) => left.lessThanOrEqual(l, x)
        case (Inr(r), x: CNil) => right.lessThanOrEqual(r, x)
      }, {
        case (Inl(l), x: CNil) => true.asRight
        case (Inr(r), x: CNil) => true.asRight
      }
    )

  implicit def cNilCoproductTransform[L, R <: Coproduct, CNil]
    : Aux[CNil, L :+: R] =
    instance(
      {
        case (x: CNil, Inl(l)) => true.asRight
        case (x: CNil, Inr(r)) => true.asRight
      }, {
        case (x: CNil, Inl(l)) => {
          print("queeeeeeeeeeeeeeeeeeeeee", x)
          true.asRight
        }
        case (x: CNil, Inr(r)) => true.asRight
      }
    )

  implicit def genericTransform[A, B, ARepr, BRepr](
      implicit
      gen: Generic.Aux[A, ARepr],
      gen1: Generic.Aux[B, BRepr],
      cch: Lazy[Aux[ARepr, BRepr]]): Aux[A, B] =
    instance(
      (a, b) => cch.value.lessThanOrEqual(gen.to(a), gen1.to(b)),
      (a, b) => cch.value.moreThanOrEqual(gen.to(a), gen1.to(b))
    )
}
