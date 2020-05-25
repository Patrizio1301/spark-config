package transformation

import com.typesafe.scalalogging.LazyLogging

trait Parameters extends LazyLogging{

  implicit class ColumnNameParser(columnName: String) {

    /** This method removes those quotes so spark can find the column correctly* Transform '"columnName"' into 'columnName'
      *
      * @return column name without being enclosed in quotation marks
      */
    def toColumnName: String = columnName.replaceAll("^\"|\"$", "")
  }

  implicit class ParameterFunction[T](parameter: Option[T]) {
    def setDefault(defaultValue: T, name: String): T = {
      Some
      parameter match {
        case Some(value) => value
        case _ => {
          logger.warn(s"The parameter $name is not defined. The default value ($defaultValue) will be applied.")
          defaultValue
        }
      }
    }
  }

}
