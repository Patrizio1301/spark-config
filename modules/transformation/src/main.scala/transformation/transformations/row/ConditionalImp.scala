package transformation.transformations.row

import transformation.errors.{MissingValue, TransformationError}
import transformation.services.InitNullsService
import transformation.transformations.Conditional
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import utils.implicits.DataFrame._
import cats.implicits._
import utils.conversion.ConversionTypeUtil
import transformation.Transform._
import transformation.Transform

object ConditionalImp extends ConversionTypeUtil {

  object ConditionalInstance extends ConditionalInstance

  trait ConditionalInstance {
    implicit val ConditionalTransformation: Transform[Conditional] = instance(
      (op: Conditional, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: Conditional, df: DataFrame): Either[TransformationError, DataFrame] = {

    val df_withCol = InitNullsService(
      df = df,
      fields = Seq(op.field),
      default = Some(op.default),
      defaultType = Some(op.dataType)
    )

    op.expressions.foldRight(df_withCol) { (current, df) =>
      val condition = current.condition
      (current.value, current.field) match {
        case (Some(givenValue), _) =>
          val statement =
            expressionByValue(condition, givenValue, op.field, op.dataType.asInstanceOf[String])
          df.map(df2 => columnWiseTransformation(df2, statement, op.field))
        case (_, Some(_field)) =>
          val statement = expressionByField(condition, _field, op.field)
          df.map(df2 => columnWiseTransformation(df2, statement, op.field))
        case (_, _) =>
          MissingValue("Conditional", "Neither a value nor a column is determined.").asLeft
      }
    }
  }

  def columnWiseTransformation(df: DataFrame, statement: String, field: String): DataFrame = {
    df.withNestedColumn(field, expr(statement))
  }

  def expressionByValue(condition: String,
                        value: String,
                        newColumn: String,
                        dataType: String): String = {
    s"case when $condition then cast('$value' as $dataType) else $newColumn end"
  }

  def expressionByField(condition: String, column: String, newColumn: String): String = {
    s"case when $condition then $column else $newColumn end"
  }
}
