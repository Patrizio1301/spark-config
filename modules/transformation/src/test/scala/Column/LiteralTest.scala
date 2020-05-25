package Column

import java.util.Date

import transformation.transformations.Literal
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
class LiteralTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("literal column with cast to String") {
    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "X",
      defaultType = Some("string")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("X")
    }
  }

  test("literal column with null default value") {

    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = null,
      defaultType = Some("string")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(null)
    }
  }

  test("literal column with empty string") {

    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "",
      defaultType = Some("string")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("")
    }
  }

  test("literal column with cast to int") {

    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "0",
      defaultType = Some("int")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(0)
    }
  }

  test("literal column with cast to Long") {

    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "0",
      defaultType = Some("long")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(0L)
    }
  }

  test("literal column with cast to Double") {
    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "0",
      defaultType = Some("double")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(0D)
    }
  }

  test("literal column with cast to Float") {
    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "0",
      defaultType = Some("float")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(0F)
    }
  }

  test("literal column with cast to Boolean") {

    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "true",
      defaultType = Some("boolean")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(true)
    }
  }

  test("literal column with cast to Date") {

    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "2017-01-01",
      defaultType = Some("date")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      assert(
        result
          .select(s"$FIELD_1")
          .collect()
          .forall(
            _.getAs[Date](s"$FIELD_1").toString == "2017-01-01"
          ))
    }
  }

  test("literal column with cast to Decimal with scale") {

    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "200.22",
      defaultType = Some("decimal(10,2)")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(new java.math.BigDecimal("200.22"))
    }
  }

  test("literal column with cast to Decimal without scale") {

    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "200.22",
      defaultType = Some("decimal(10)")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(new java.math.BigDecimal("200"))
    }
  }

  test("literal column with cast to unknown type") {
    val content = Seq(
      Row("aa"),
      Row("bb"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Literal(
      field = FIELD_1,
      default = "unknown",
      defaultType = Some("unknown")
    ).transform(df)

    dfResult.isLeft shouldBe true

    dfResult.leftMap { error =>
      error.toString shouldBe "Error in transformation Literal: " +
        "The paramater defaultType has the invalid value unknown. " +
        "Conversion Util Error: Format unknown has not been implemented."
    }
  }
}
