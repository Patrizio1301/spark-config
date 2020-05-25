package output.errors

sealed trait OutputError

final case class MissingValue(output: String, param: String) extends OutputError {
  override def toString: String =
    s"Error in input.output $output: The parameter $param is missing."
}

final case class EmptyError(output: String, param: String, message: String="")
  extends OutputError {
  override def toString: String =
    s"Error in input.output $output: The parameter $param is an empty list. $message"
}

final case class ConversionError(output: String, message: String)
    extends OutputError {
  override def toString: String =
    s"Error in input.output $output: $message"
}

final case class OperationError(output: String, message: String)
    extends OutputError {
  override def toString: String =
    s"Error in input.output $output: $message"
}

final case class UnexpectedError(output: String, message: String)
  extends OutputError {
  override def toString: String =
    s"Error in input.output $output: The transformation caused the following unexpected error : $message"
}

final case class InvalidValue(output: String, param: String, value: String, message: String="")
    extends OutputError {
  override def toString: String =
    s"Error in input.output $output: The paramater $param has the invalid value $value. $message"
}

final case class MissingField(output: String, field: String)
  extends OutputError {
  override def toString: String =
    s"Error in input.output $output: The field $field is not contained in the current dataFrame."
}

