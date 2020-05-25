package Column

import cats.implicits._
import transformation.transformations.CaseLetter
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
class CaseLetterTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("upper case") {

    val content = Seq(
      Row("aa"),
      Row("bB"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = CaseLetter(
      field = FIELD_1,
      operation = "upper"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("AA", "BB", "")
    }
  }

  test("lower case") {

    val content = Seq(
      Row("aa"),
      Row("bB"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = CaseLetter(
      field = FIELD_1,
      operation = "lower"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("aa", "bb", "")
    }
  }

  test("Invalid Case") {

    val content = Seq(
      Row("aa"),
      Row("bB"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = CaseLetter(
      field = FIELD_1,
      operation = "invalid"
    ).validate.map(op => op.transform(df))

    dfResult.isLeft shouldBe true

    dfResult.leftMap { result =>
      result.toString() shouldBe "Error in transformation CaseLetter: " +
        "The paramater operation has the invalid value invalid. " +
        "Valid values are: upper, lower."
    }
  }

  test("with nested structure level 1") {

    val content = Seq(Row(Row("AAA"), "cc", "AAA"))
    val df      = createDataFrame(content, NestedTwoLevels(3, 1))

    val dfResult = CaseLetter(
      field = s"$FIELD_1.$FIELD_1",
      operation = "lower"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, s"$FIELD_1.$FIELD_1") shouldBe Set("aaa")
    }
  }

  test("with nested structure level 2") {
    val content = Seq(Row(Row(Row("DDD"), "cc", "AAA"), "BB", "AA"))
    val df =
      createDataFrame(content, nestedSchema(Seq(StringType, StringType, StringType), Seq(3, 3, 1)))
    val dfResult = CaseLetter(
      field = s"$FIELD_1.$FIELD_1.$FIELD_1",
      operation = "lower"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, s"$FIELD_1.$FIELD_1.$FIELD_1") shouldBe Set("ddd")
    }
  }
}
