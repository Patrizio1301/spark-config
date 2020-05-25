package Column

import transformation.transformations.Integrity
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import transformation.Transformations._
import transformation.Transform._
import cats.implicits._

@RunWith(classOf[JUnitRunner])
class IntegrityTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("Return correct integrity parse") {
    testCase = "CD-48"

    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("other")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Integrity(
      field = FIELD_1,
      path = "src/test/resources/transformation.transformations/integrity",
      default = "00"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("aa", "bb", "00")
    }
  }

  test("Return correct integrity parser change value") {
    testCase = "CD-43"

    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Integrity(
      field = FIELD_1,
      path = "src/test/resources/transformation.transformations/integrity",
      default = null
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("aa", "bb", null)
    }
  }
}
