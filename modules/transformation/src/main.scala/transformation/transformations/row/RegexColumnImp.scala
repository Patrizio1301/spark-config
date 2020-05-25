package transformation.transformations.row

import transformation._
import transformation.errors.TransformationError
import org.apache.spark.sql.DataFrame
import cats.implicits._
import transformation.Parameters
import transformation.transformations.RegexColumn
import org.apache.spark.sql.functions.regexp_extract
import transformation.{Parameters, Transform}

/** Return or overwrite a column after applying a regex pattern in a origin column
  *
  * @param config config for RegexColumn masterization.
  */
object RegexColumnImp extends Parameters {
  import Transform._
  object RegexcolumnInstance extends RegexcolumnInstance

  trait RegexcolumnInstance {
    implicit val RegexcolumnTransformation: Transform[RegexColumn] = instance(
      (op: RegexColumn, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: RegexColumn, df: DataFrame): Either[TransformationError, DataFrame] = {
    import utils.implicits.DataFrame._
    op.regex.foldLeft(Right(df): Either[TransformationError, DataFrame]) {
      case (dfWithNestedColumn, _regex) =>
        dfWithNestedColumn.map { df =>
          df.withNestedColumn(
            _regex.field,
            regexp_extract(df.col(op.columnToRegex), op.regexPattern, _regex.regexGroup))
        }
    }
  }

}
