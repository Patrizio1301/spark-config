package transformation.transformations.row

import transformation._
import transformation.errors.{MissingValue, TransformationError}
import org.apache.spark.sql.DataFrame
import cats.implicits._
import transformation.Parameters
import transformation.transformations.SelectColumns
import transformation.{Parameters, Transform}

object SelectColumnsImp extends Parameters {
  import Transform._
  object SelectcolumnsInstance extends SelectcolumnsInstance

  trait SelectcolumnsInstance {
    implicit val SelectcolumnsTransformation: Transform[SelectColumns] = instance(
      (op: SelectColumns, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: SelectColumns, df: DataFrame): Either[TransformationError, DataFrame] = {
    logger.info(s"SelectColumns: selected columns: ${op.columnsToSelect.mkString(", ")}")
    logger.debug(s"SelectColumns: original columns: ${df.columns.mkString(", ")}")
    val _columnsToSelect = op.columnsToSelect.map(_.toColumnName)
    df.select(_columnsToSelect.map(df(_)): _*).asRight
  }

  def validated(
      columnsToSelect: Seq[String]
  ): Either[TransformationError, SelectColumns] = {

    for {
      validatedFields <- validateColumnsToSelect(columnsToSelect: Seq[String])

    } yield new SelectColumns(validatedFields)
  }

  def validateColumnsToSelect(
      columnsToSelect: Seq[String]
  ): Either[TransformationError, Seq[String]] = {
    // TODO: ERROR HANDLING
    Either.cond(
      columnsToSelect.nonEmpty,
      columnsToSelect,
      MissingValue("SelectColumns", "columnsToSelect")
    )
  }
}
