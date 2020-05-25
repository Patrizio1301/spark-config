package utils.Formatter

import scala.util.matching.Regex

object LogicalFormatRegex {
  val decimalPattern: Regex      = """DECIMAL *\(([0-9]*)\)( *\(([0-9]*)\))?""".r
  val decimalPattern2: Regex     = """DECIMAL *\(([0-9]*)\,([0-9]+)\)( *\(([0-9]*)\))?""".r
  val charPattern: Regex         = """ALPHANUMERIC *\(([0-9]*)\)( *\(([0-9]*)\))?""".r
  val numericLargePattern: Regex = """NUMERIC LARGE( *\(([0-9]*)\))?""".r
  val numericShortPattern: Regex = """NUMERIC SHORT( *\(([0-9]*)\))?""".r
  val datePattern: Regex         = """DATE( *\(([0-9]*)\))?""".r
  val timePattern: Regex         = """TIME( *\(([0-9]*)\))?""".r
  val timestampPattern: Regex    = """TIMESTAMP( *\(([0-9]*)\))?""".r
}
