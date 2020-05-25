package transformation.transformations

import transformation.errors.{InvalidValue, TransformationError}

object Validator extends Validator

class Validator {

  def domainValidation[T](
                           transformation: String,
                           operation: String,
                           domain: Seq[T],
                           element: T,
                           StringLower: Boolean = true
                         ): Either[TransformationError, T] = {
    Either.cond(
      domain.contains(element),
      element,
      InvalidValue(
        transformation,
        operation,
        element.toString,
        s"Valid values are: ${domain.mkString(", ")}."
      )
    )
  }
}
