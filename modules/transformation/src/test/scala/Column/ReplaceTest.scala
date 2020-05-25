package Column

import transformation.transformations.Replace
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import transformation.Transformations._
import cats.implicits._
import transformation.Transform._

@RunWith(classOf[JUnitRunner])
class ReplaceTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("replace data in column") {
    val content = Seq(
      Row("change1"),
      Row("change2"),
      Row("conserved"),
      Row(null)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Replace(
      field = FIELD_1,
      replace = Map(("change1", "good1"), ("change2", "good2"))
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("good1", "good2", "conserved", null)
    }
  }
}
