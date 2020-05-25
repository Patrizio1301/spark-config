package Column

import transformation.transformations.CharacterTrimmer
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import transformation.Transform._
import transformation.Transformations._

@RunWith(classOf[JUnitRunner])
class CharacterTrimmerTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("Remove starting zeros") {

    val content =
      Seq(
        Row("0000182"),
        Row("000080"),
        Row("10000120000"),
        Row("002212000"),
        Row("12")
      )

    val df = createDataFrame(content, inputSchema(1, StringType))

    When("apply transformation.transformations")
    val dfResult = CharacterTrimmer(
      field = FIELD_1,
      characterTrimmer = Some("0")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("182", "80", "10000120000", "2212000", "12")
    }
  }

  test("Remove ending zeros") {

    val content = Seq(
      Row("0000182"),
      Row("000080"),
      Row("10000120000"),
      Row("002212000"),
      Row("12"),
      Row(null)
    )
    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = CharacterTrimmer(
      field = FIELD_1,
      characterTrimmer = Some("0"),
      trimType = Some("right")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("0000182",
                                                   "00008",
                                                   "1000012",
                                                   "002212",
                                                   "12",
                                                   null)
    }
  }

  test("Remove starting and ending zeros") {

    val content = Seq(
      Row("0000182"),
      Row("000080"),
      Row("10000120000"),
      Row("002212000"),
      Row("12")
    )
    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = CharacterTrimmer(
      field = FIELD_1,
      characterTrimmer = Some("0"),
      trimType = Some("both")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("182", "8", "1000012", "2212", "12")
    }
  }
}
