package utils.SchemaFields

object MetadataFields {

  /**
    * logical format of the field, could be different of physical type
    */
  val LOGICALFORMAT = "logicalFormat"

  /**
    * date pattern of the field (to parse dates in string)
    */
  val FORMAT = "format"

  /**
    * locale of the field (to parse dates in string or numerics in string)
    */
  val LOCALE = "locale"

  /**
    * autorename field flag
    */
  val RENAME = "rename"

  /**
    * flag to specify if the field is token (or must be token)
    */
  val TOKENIZED = "tokenized"

  /**
    * tokenization method used in the field
    */
  val TOKENIZATIONMETHOD = "tokenizationMethod"

  /**
    * origin for tokenization
    */
  val ORIGIN = "origin"

  /**
    * flag to spacify if a field is optional in validation stage
    */
  val OPTIONAL = "optional"

  /**
    * This metadata is added to support HOST binary types. Used by CobolInput to build the final copybook used by cobrix
    */
  val TYPE = "type"

  /**
    * default value of the field (written as a string)
    */
  val DEFAULT = "default"

  /**
    * This field is ignored from parse and validation routines (except parse from raw)
    */
  val DELETED = "deleted"

  /**
    *
    */
  val METADATA = "metadata"
}
