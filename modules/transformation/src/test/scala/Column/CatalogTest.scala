package Column

import transformation.transformations.Catalog
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import transformation.Transform._
import transformation.Transformations._

@RunWith(classOf[JUnitRunner])
class CatalogTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("check correct parse with catalog") {
    val content = Seq(
      Row("aa"),
      Row("bB"),
      Row(""),
      Row(null)
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Catalog(
      path = "modules/transformation/src/test/resources/dictionary.txt",
      field = FIELD_1
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("bB", "world", "", null)
    }
  }
}
