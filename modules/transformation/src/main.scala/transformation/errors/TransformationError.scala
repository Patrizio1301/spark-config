package transformation.errors

sealed trait TransformationError

final case class MissingValue(transformation: String, param: String) extends TransformationError {
  override def toString: String =
    s"Error in transformation $transformation: The parameter $param is missing."
}

final case class EmptyError(transformation: String, param: String, message: String="")
  extends TransformationError {
  override def toString: String =
    s"Error in transformation $transformation: The parameter $param is an empty list. $message"
}

final case class ConversionError(transformation: String, message: String)
    extends TransformationError {
  override def toString: String =
    s"Error in transformation $transformation: $message"
}

final case class OperationError(transformation: String, message: String)
    extends TransformationError {
  override def toString: String =
    s"Error in transformation $transformation: $message"
}

final case class UnexpectedError(transformation: String, message: String)
  extends TransformationError {
  override def toString: String =
    s"Error in transformation $transformation: The transformation caused the following unexpected error : $message"
}

final case class InvalidValue(transformation: String, param: String, value: String, message: String="")
    extends TransformationError {
  override def toString: String =
    s"Error in transformation $transformation: The paramater $param has the invalid value $value. $message"
}

final case class MissingField(transformation: String, field: String)
  extends TransformationError {
  override def toString: String =
    s"Error in transformation $transformation: The field $field is not contained in the current dataFrame."
}

