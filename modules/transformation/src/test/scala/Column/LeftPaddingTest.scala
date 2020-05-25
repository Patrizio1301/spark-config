package Column

import transformation.transformations.LeftPadding
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
class LeftPaddingTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("fill with zeros a correct office number") {

    val content = Seq(
      Row(182),
      Row(8),
      Row(null),
      Row(12),
      Row(2212)
    )
    val df = createDataFrame(content, inputSchema(1, IntegerType))

    val dfResult = LeftPadding(
      field = FIELD_1,
      lengthDest = Some(4),
      fillCharacter = Some("0")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("0182", "0008", "0012", "2212", null)
      result.schema.map(_.dataType) shouldBe Seq(StringType)
    }
  }

  test("fill with zeros a correct office number using default for null") {

    val content = Seq(
      Row(182),
      Row(8),
      Row(null),
      Row(12),
      Row(2212)
    )

    val df = createDataFrame(content, inputSchema(1, IntegerType))

    val dfResult = LeftPadding(
      field = FIELD_1,
      lengthDest = Some(4),
      fillCharacter = Some("0"),
      nullValue = Some("nullValue")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("0182", "0008", "0012", "2212", "nullValue")
      result.schema.map(_.dataType) shouldBe Seq(StringType)
    }
  }

  test("fill with zeros a correct office number 2") {
    val content = Seq(
      Row(182),
      Row(8),
      Row(12),
      Row(2212)
    )
    val df = createDataFrame(content, inputSchema(1, IntegerType))

    val dfResult = LeftPadding(
      field = FIELD_1,
      lengthDest = Some(4),
      fillCharacter = Some("0")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("0182", "0008", "0012", "2212")
      result.collect().length shouldBe 4
    }
  }
}
