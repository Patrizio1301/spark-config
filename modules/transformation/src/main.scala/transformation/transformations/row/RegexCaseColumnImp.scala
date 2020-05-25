package transformation.transformations.row

import transformation._
import transformation.errors.{MissingValue, TransformationError}
import cats.implicits._
import transformation.Transform
import transformation.transformations.{RegexCaseColumn, RegexConfig}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import transformation.{Parameters, Transform}

import scala.util.{Failure, Success, Try}

/** Return or overwrite a column filling values using the regex pattern in the original column.
  *
  * NOTE: Regex priority goes from first to last
  * This means that if the pattern for the first and third regex are both found, the value in the column will correspond to the first regex
  *
  * Default is meaningless if field is a column that already exists, since it will default to the values already in that column
  *
  * @param config a config containing regexcasecolumn.
  */
object RegexCaseColumnImp extends Parameters {
  import Transform._
  object RegexcasecolumnInstance extends RegexcasecolumnInstance

  trait RegexcasecolumnInstance {
    implicit val RegexcasecolumnTransformation: Transform[RegexCaseColumn] = instance(
      (op: RegexCaseColumn, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: RegexCaseColumn, df: DataFrame): Either[TransformationError, DataFrame] = {
    import utils.implicits.DataFrame._
    addDefaultColumnIfNeeded(op, df).map { col =>
      op.regexList.foldRight(df.withNestedColumn(op.field, col)) { (regexInfo, col) =>
        col.withNestedColumn(op.field,
                             when(col(regexInfo.columnToRegex).rlike(regexInfo.pattern),
                                  lit(regexInfo.value)).otherwise(col(op.field)))
      }
    }
  }

  private def addDefaultColumnIfNeeded(op: RegexCaseColumn,
                                       df: DataFrame): Either[TransformationError, Column] = {
    val _default: String = op.default.setDefault("", "default")
    Try(df(op.field)) match {
      case Success(col) =>
        logger.debug(s"Transformer: Use existing column: ${op.field}")
        col.asRight
      case Failure(_) =>
        logger.debug(s"Transformer: Create new column ${op.field}")
        lit(_default).asRight
    }
  }
  def validated(
      field: String,
      default: Option[String] = None,
      regexList: Seq[RegexConfig]
  ): Either[TransformationError, RegexCaseColumn] = {

    for {
      validatedregexList <- validateRegexList(regexList)

    } yield
      RegexCaseColumn(
        field,
        default,
        validatedregexList
      )
  }

  def validateRegexList(
      regexList: Seq[RegexConfig]
  ): Either[TransformationError, Seq[RegexConfig]] =
    Either.cond(
      regexList.nonEmpty,
      regexList,
      MissingValue(
        "RegexCaseColumn",
        "These operations cannot be performed on one or zero columns." +
          " Add two or more column names into valuesToOperateOn: [<column1>, <column2>]"
      )
    )

}
