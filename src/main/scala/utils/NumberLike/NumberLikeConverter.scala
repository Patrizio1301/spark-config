package utils.NumberLike

import shapeless.{CNil, Coproduct}
import utils.NumberLike.NumberLikeType.NumberLikeType

trait NumberLikeConverter[A] {
    def apply(a: A): NumberLikeType
}

object NumberLikeConverter {

  implicit object nullToNumberLike extends NumberLikeConverter[Null] {
    def apply(n: Null): NumberLikeType = Coproduct[NumberLikeType](n)
  }

  implicit object IntToNumberLike extends NumberLikeConverter[Int] {
    def apply(n: Int): NumberLikeType = Coproduct[NumberLikeType](n)
  }

  implicit object DoubleToNumberLike extends NumberLikeConverter[Double] {
    def apply(n: Double): NumberLikeType = Coproduct[NumberLikeType](n)
  }

  implicit object FloatToNumberLike extends NumberLikeConverter[Float] {
    def apply(n: Float): NumberLikeType = Coproduct[NumberLikeType](n)
  }

  implicit object SqlDateToNumberLike extends NumberLikeConverter[java.sql.Date] {
    def apply(n: java.sql.Date): NumberLikeType =
      Coproduct[NumberLikeType](new java.sql.Timestamp(n.getTime))
  }

  implicit object UtilDateToNumberLike extends NumberLikeConverter[java.util.Date] {
    def apply(n: java.util.Date): NumberLikeType =
      Coproduct[NumberLikeType](new java.sql.Timestamp(n.getTime))
  }

  implicit object TimestampToNumberLike extends NumberLikeConverter[java.sql.Timestamp] {
    def apply(n: java.sql.Timestamp): NumberLikeType = Coproduct[NumberLikeType](n)
  }
}
