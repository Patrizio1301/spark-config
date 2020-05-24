package utils.implicits

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types._
import com.datio.kirby.implicits.Columns._
import com.datio.kirby.implicits.StructTypes._
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types._

object DataFrame {

  implicit class ImplicitFunction(df: DataFrame) extends Serializable {

    /**
      * Returns a new DataFrame by adding a column or replacing the existing column that has
      * the same name. Nested structures will be created or updated, if necessary.
      *
      * @param newColName     Column name of the new column.
      * @param newCol         New column
      * @return               Dataframe with the new column included.
      */
    def withNestedColumn(newColName: String, newCol: Column): DataFrame = {
      if (newColName.contains('.')) {
        val splitted = newColName.split('.')
        val modifiedOrAdded: Column = df.schema.fields
          .find(_.name == splitted.head)
          .map(f => recursiveDetection(df, f.dataType, f.name, f.nullable, "", splitted, newCol))
          .getOrElse {
            newCol.createNestedStructs(splitted.tail) as splitted.head
          }
        df.withColumn(splitted.head, modifiedOrAdded)
      } else {
        df.withColumn(newColName, newCol)
      }
    }

    /**
      * Returns a new DataFrame by dropping the column. If the column include '.' nested structures will be
      * dropped and updated.
      *
      * @param field    field which is required to be dropped
      * @return         DataFrame without *field*
      */
    def dropNestedColumn(field: String): DataFrame = {
      val parent = field.split('.').dropRight(1).mkString(".")
      df.schema.getBrothers(field).isEmpty match {
        case true if field.split('.').length > 1 => df.dropNestedColumn(parent)
        case _ =>
          parentAndField(field) match {
            case (None, _) => df.drop(field)
            case (Some(parent), field) =>
              val otherStructFields =
                df.select(s"$parent.*").columns.filter(_ != field).map(v => col(s"$parent.$v"))
              val newColumn = df.schema
                .unnested()
                .fields
                .filter(_.name == parent)
                .map(_.nullable)
                .reduce(_ && _) match {
                case true  => struct(otherStructFields: _*).nullableCol()
                case false => struct(otherStructFields: _*)
              }
              if (parent.contains('.')) {
                df.withNestedColumn(parent, newColumn)
              } else {
                if (newColumn == struct()) {
                  df.drop(parent)
                } else {
                  df.withColumn(parent, newColumn)
                }
              }
          }
      }
    }

    /**
      * Returns a new DataFrame by dropping the columns. If the columns include '.' nested structures will be
      * dropped and updated.
      *
      * @param field    fields which are required to be dropped
      * @return         DataFrame without *fields*
      */
    def dropNestedColumn(colNames: String*): DataFrame = {
      val (nestedColumns, notNestedColumns) = colNames.span(_.contains("."))
      nestedColumns.foldLeft(df.drop(notNestedColumns: _*))((df, nestedColumn) =>
        df.dropNestedColumn(nestedColumn))
    }

    /**
      * Returns a new Dataset with a column renamed.
      * This is a no-op if schema doesn't contain existingName.
      *
      */
    def withNestedColumnRenamed(existingName: String, newName: String): DataFrame = {
      parentAndField(newName) match {
        case (None, _) => df.withColumnRenamed(existingName, newName)
        case (Some(_), _) =>
          existingName.startsWith(newName) match {
            case false =>
              df.withNestedColumn(newName, df(existingName)).dropNestedColumn(existingName)
            case true =>
              def check(name: String, parent: String): Boolean = {
                df.schema.getBrothers(name) match {
                  case Seq()
                    if name.startsWith(parent) && parent
                      .split('.')
                      .length <= name.split('.').length - 2 =>
                    check(name.split('.').dropRight(1).mkString("."), parent)
                  case Seq()
                    if name.startsWith(parent) && parent
                      .split('.')
                      .length == name.split('.').length - 1 =>
                    true
                  case _ => false
                }
              }
              check(existingName, newName) match {
                case false =>
                  throw new Exception(
                    s"Column $newName with nested structure cannot be overwritten!")
                case true =>
                  val df_aux = df
                    .withNestedColumn(s"${newName}_aux", df(existingName))
                    .dropNestedColumn(existingName)
                  df_aux
                    .withNestedColumn(newName, df_aux(s"${newName}_aux"))
                    .dropNestedColumn(s"${newName}_aux")
              }
          }
      }
    }

    /**
      * Change the nullable argument in the dataframes schema for a specific column.
      *
      * @param column
      * @param nullable
      * @return
      */
    import org.apache.spark.sql.types._
    def setNullableStateForColumn(column: String, nullable: Boolean): DataFrame = {
      val newSchema = StructType(df.schema.unnested().map {
        case element if element.name == column => element.copy(nullable = nullable)
        case y: StructField                    => y
      })
      df.sqlContext.createDataFrame(df.rdd, newSchema.nested())
    }

  }

  /**
    *  Two cases are considered here:
    *  A) The field is structType. In this case, a recursive process is necessary in order to add the new column.
    *  B) The field is a simple type column. In this case, no new column can be added and an exception will be reported.
    *
    * @param field      Field which will be actualized.
    * @param splitted   New column name splitted by ".".
    * @param column     New column to be added.
    * @return           The field, but actualized (new column will be added here).
    */
  def recursiveDetection(
                          df: DataFrame,
                          dataType: DataType,
                          name: String,
                          nullable: Boolean,
                          parent: String,
                          splitted: Array[String],
                          column: Column
                        ): Column = {

    val newParent = if (parent != "") s"$parent.${name}" else name

    val recursive = (dataType: DataType) =>
      recursiveDetection(
        df,
        dataType,
        name,
        nullable,
        parent,
        splitted,
        column
      )

    dataType match {
      case colType: StructType =>
        col(newParent)
          .recursiveAddNestedColumn(
            df,
            newParent,
            splitted,
            colType,
            nullable,
            column
          )
      case colType: ArrayType => recursive(colType.elementType)
      case colType: MapType   => recursive(colType.valueType)
      case _ if newParent == splitted.dropRight(1).mkString(".") => {
        throw new Exception(
          s"Add Column: The column ${splitted.mkString(".")} cannot be added, because of the current schema structure.")
      }
      case _ => df(newParent)
    }
  }

  /** split the column in parent and field
    *
    * @return tuple(Option[parent], field)
    */
  def parentAndField(columnName: String): (Option[String], String) = {
    columnName.split("\\.(?!.*\\.)").toList match {
      case parent :: field :: Nil => (Some(parent), field)
      case field :: Nil           => (None, field)
    }
  }

}