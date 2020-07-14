package validation.types

import shapeless.{:+:, CNil}

object types {
  type Numerical = Int :+: Double :+: Float :+: CNil
  type multipleFields = String :+: Seq[String] :+: CNil
}
