package transformation.transformations.row

import cats.implicits._
import transformation._
import transformation.errors.{MissingField, TransformationError, UnexpectedError}
import transformation.transformations.ConcatColumns
import com.typesafe.scalalogging.LazyLogging
import transformation.Transform
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import transformation.{Parameters, Transform}

import scala.util.{Failure, Success, Try}

/** This class concatenates columns by
  *
  * @param config concat columns.
  */
object ConcatColumnsImp extends Parameters {

  import Transform._

  object ConcatcolumnsInstance extends ConcatcolumnsInstance

  trait ConcatcolumnsInstance {
    implicit val ConcatcolumnsTransformation: Transform[ConcatColumns] = instance(
      (op: ConcatColumns, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: ConcatColumns, df: DataFrame): Either[TransformationError, DataFrame] = {

    import utils.implicits.DataFrame._
    import utils.implicits.StructTypes._

    val _separator: String = op.separator.setDefault("", "separator")

    val columns = df.schema.unnested().fields.map(field => field.name)
    Try(op.columnsToConcat
      .foldLeft(Right(Seq[String]()): Either[TransformationError, Seq[String]]) { (seq, field) =>
        columns.contains(field) match {
          case false => MissingField("ConcatColumns", field).asLeft
          case true  => seq.map(sequence => sequence ++ Seq(field))
        }
      }
      .map { _columnsToConcat: Seq[String] =>
        op.convertNulls match {
          case Some(convertion) =>
            logger.info(
              s"ConcatColumns: concatenating columns: '${_columnsToConcat.mkString(",")}', " +
                s"with nulls converted to $convertion, using separator '${_separator}'.")
            val concatUdf = udf[String, String, String, Seq[Any]](concatWithoutNulls)
            df.withNestedColumn(
              op.columnName,
              concatUdf(lit(_separator), lit(convertion), array(_columnsToConcat.map(col): _*)))
          case _ =>
            logger.info(s"ConcatColumns: concatenating columns: '${_columnsToConcat
              .mkString(",")}' using separator '${_separator}'.")
            df.withNestedColumn(op.columnName, concat_ws(_separator, _columnsToConcat.map(col): _*))
        }
      }) match {
      case Success(df)        => df
      case Failure(exception) => UnexpectedError("ConcatColumns", exception.getMessage).asLeft
    }
  }

  private def concatWithoutNulls(separator: String,
                                 convertNulls: String,
                                 valuesToConcat: Seq[Any]): String = {
    valuesToConcat.map(value => if (value == null) convertNulls else value).mkString(separator)
  }

}

//object Concatcolumns {
//
//  def validated(
//      columnsToConcat: Seq[String],
//      columnName: String,
//      separator: Option[String] = None,
//      convertNulls: Option[String] = None
//  ): Either[TransformationError, Concatcolumns] = {
//    Right(
//      Concatcolumns(
//        columnsToConcat,
//        columnName,
//        separator,
//        convertNulls
//      ))
//  }
//
//}
