package transformation.transformations.row

import transformation._
import transformation.errors.{InvalidValue, MissingValue, TransformationError}
import transformation.services.InitNullsService
import org.apache.spark.sql.DataFrame
import cats.implicits._
import transformation.Transform
import transformation.transformations.InitNulls
import transformation.{Parameters, Transform}

/** Initialize nulls with default value.
  * Because it's necessary use the DataFrame object to do the init nulls task, we have overridden the method
  * transform(DataFrame) to use the DataFrame directly, then we don't need to use the method transform(Column)
  *
  * @param config config for init null masterization.
  */
object InitNullsImp extends Parameters {
  import Transform._
  object InitNullsInstance extends InitNullsInstance

  trait InitNullsInstance {
    implicit val InitNullsTransformation: Transform[InitNulls] = instance(
      (op: InitNulls, df: DataFrame) => transformation(op, df)
    )
  }

  /**
    * Transform fullfills all empty entries in all fields selected (in list field) with the default value.
    *
    * @param df DataFrame to be transformed.
    * @return Dataframe with null-entries fullfilled with default.
    */
  def transformation(op: InitNulls, df: DataFrame): Either[TransformationError, DataFrame] = {
    val allFields = op.field match {
      case Some(f) => (op.fields ++ Seq(f)).map(_.toColumnName)
      case _       => op.fields.map(_.toColumnName)
    }
    InitNullsService(df = df,
                     fields = allFields,
                     default = Some(op.default),
                     defaultType = Some(op.defaultType))
  }

  def validated(
      fields: Seq[String] = Seq(),
      default: String,
      defaultType: String,
      field: Option[String] = None
  ): Either[TransformationError, InitNulls] = {
    for {
      validatedFields <- validateFields(fields, field)
    } yield
      InitNulls(
        validatedFields,
        default,
        defaultType,
        field
      )
  }

  private def validateFields(
      fields: Seq[String],
      field: Option[String]
  ): Either[TransformationError, Seq[String]] = {
    Either.cond(
      fields.nonEmpty || field.nonEmpty,
      fields,
      MissingValue("InitNulls", "fields or field")
    )
  }

}
