package utils.NumberLike

import shapeless.{:+:, CNil}

object NumberLikeType {
  type NumberLikeType = Null :+: Int :+: Double :+: Float :+: java.sql.Timestamp :+: CNil
}
