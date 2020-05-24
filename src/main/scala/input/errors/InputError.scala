package input.errors

sealed trait InputError

final case class MissingValue(input: String, param: String) extends InputError {
  override def toString: String =
    s"Error in input $input: The parameter $param is missing."
}

final case class EmptyError(input: String, param: String, message: String="")
  extends InputError {
  override def toString: String =
    s"Error in input $input: The parameter $param is an empty list. $message"
}

final case class ConversionError(input: String, message: String)
    extends InputError {
  override def toString: String =
    s"Error in input $input: $message"
}

final case class OperationError(input: String, message: String)
    extends InputError {
  override def toString: String =
    s"Error in input $input: $message"
}

final case class UnexpectedError(input: String, message: String)
  extends InputError {
  override def toString: String =
    s"Error in input $input: The transformation caused the following unexpected error : $message"
}

final case class InvalidValue(input: String, param: String, value: String, message: String="")
    extends InputError {
  override def toString: String =
    s"Error in input $input: The paramater $param has the invalid value $value. $message"
}

final case class MissingField(input: String, field: String)
  extends InputError {
  override def toString: String =
    s"Error in input $input: The field $field is not contained in the current dataFrame."
}

