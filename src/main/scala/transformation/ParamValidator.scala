package transformation

import transformation.errors.TransformationError

trait ParamValidator[T] {
  def validate: Either[TransformationError, T]
}
