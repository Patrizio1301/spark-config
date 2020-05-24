package validation.errors

sealed trait ValidationError

final case class MissingValue(validation: String, param: String) extends ValidationError {
  override def toString: String =
    s"Error in validation $validation: The parameter $param is missing."
}

final case class EmptyError(validation: String, param: String, message: String="")
  extends ValidationError {
  override def toString: String =
    s"Error in validation $validation: The parameter $param is an empty list. $message"
}

final case class ConversionError(validation: String, message: String)
    extends ValidationError {
  override def toString: String =
    s"Error in validation $validation: $message"
}

final case class OperationError(validation: String, message: String)
    extends ValidationError {
  override def toString: String =
    s"Error in validation $validation: $message"
}

final case class UnexpectedError(validation: String, message: String)
  extends ValidationError {
  override def toString: String =
    s"Error in validation $validation: The validation caused the following unexpected error : $message"
}

final case class InvalidValue(validation: String, param: String, value: String, message: String="")
    extends ValidationError {
  override def toString: String =
    s"Error in validation $validation: The paramater $param has the invalid value $value. $message"
}

final case class InvalidType(validation: String, param: String, value: String, message: String="")
  extends ValidationError {
  override def toString: String =
    s"Error in validation $validation: The paramater $param has the invalid type $value. $message"
}

final case class MissingField(validation: String, field: String)
  extends ValidationError {
  override def toString: String =
    s"Error in validation $validation: The field $field is not contained in the current dataFrame."
}

