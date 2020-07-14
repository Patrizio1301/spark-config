package validation

trait ParamValidator[T] {
  def validation: Either[String, T]
}
