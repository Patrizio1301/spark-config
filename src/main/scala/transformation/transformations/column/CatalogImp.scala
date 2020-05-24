package transformation.transformations.column

import transformation.{ParamValidator, Parameters, Transform}
import transformation.errors.TransformationError
import cats.implicits._
import transformation.transformations.Catalog
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import transformation.Transform._

/** Change field value (key) for dictionary value
 */
/** Transform to replace columns values using a map
  */
object CatalogImp extends Parameters {

  object CatalogInstance extends CatalogInstance

  trait CatalogInstance {
    implicit val CatalogTransformation: Transform[Catalog] =
      instance((op: Catalog, col: Column) => col.asRight,
               (field, df, col, op) => transformation(field, df, col, op))
  }

  def transformation(field: String,
                             df: DataFrame,
                             col: Column,
                             op: Catalog): Either[TransformationError, DataFrame] = {
    lazy val replace: Map[String, String] = readDictionary(op.path)
    logger.info(s"Replace: column $field with map: ${replace.mkString(", ")}")
    df.na.replace(field, replace).asRight
  }

  private def readDictionary(path: String): Map[String, String] = {
    logger.debug("Masterization: Read catalog from path : {}", path)
    val spark = SparkSession.getDefaultSession.get
    import spark.implicits._

    spark.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(path)
      .map(r => (r(0).toString, r(1).toString))
      .collect()
      .toMap
  }

}
