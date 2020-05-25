package Column

import cats.implicits._
import com.typesafe.config.ConfigFactory
import transformation.transformations.InsertLiteral
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{AnalysisException, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import transformation.Transform._
import transformation.Transformations._

@RunWith(classOf[JUnitRunner])
class InsertLiteralTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("Insert a string at a position in a string from the left, where it will always work") {
    val content = Seq(
      Row("Hello"),
      Row("World"),
      Row("I am Calculatron")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = InsertLiteral(
      field = FIELD_1,
      value = "Hello",
      offset = 3,
      offsetFrom = Some("left")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(
        "HelHellolo",
        "WorHellold",
        "I aHellom Calculatron"
      )
    }
  }

  test("Insert a string at a position in a string from the right, where it will always work") {
    val content = Seq(
      Row("Hello"),
      Row("World"),
      Row("I am Calculatron")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = InsertLiteral(
      field = FIELD_1,
      value = "Hello",
      offset = 3,
      offsetFrom = Some("right")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("HeHellollo",
                                                   "WoHellorld",
                                                   "I am CalculatHelloron")
    }
  }

  test("Insert a string at a position outside of the strings in the data") {

    val content = Seq(
      Row("I"),
      Row("am"),
      Row("")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = InsertLiteral(
      field = FIELD_1,
      value = "Hello",
      offset = 5,
      offsetFrom = Some("left")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("IHello", "amHello", "Hello")
    }
  }

  test("Insert a string using the default position and offset") {

    val content = Seq(
      Row("Hello"),
      Row("World"),
      Row("I am Calculatron")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = InsertLiteral(
      field = FIELD_1,
      value = "Hello "
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(
        "Hello Hello",
        "Hello World",
        "Hello I am Calculatron"
      )
    }
  }

  test("Insert a string at a position in an integer column") {
    val content = Seq(
      Row(1),
      Row(9),
      Row(3)
    )

    val df = createDataFrame(content, inputSchema(1, IntegerType))

    val dfResult = InsertLiteral(
      field = FIELD_1,
      value = "Hello"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(
        "Hello1",
        "Hello9",
        "Hello3"
      )
    }
  }

  test("Insert a string at a position in a double column") {
    val content = Seq(
      Row(0.9),
      Row(7.2),
      Row(5.1)
    )

    val df = createDataFrame(content, inputSchema(1, DoubleType))

    val dfResult = InsertLiteral(
      field = FIELD_1,
      value = "Hello"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(
        "Hello0.9",
        "Hello7.2",
        "Hello5.1"
      )
    }
  }

  test("Insert a string at a position in a column with nulls") {
    val content = Seq(
      Row(null),
      Row(null),
      Row("Mathematical")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = InsertLiteral(
      field = FIELD_1,
      value = "Hello"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("Hello", "HelloMathematical")
    }
  }

  test("Insert a string at a position in a column with nulls using the right side transform") {

    val content = Seq(
      Row(null),
      Row(null),
      Row("Mathematical")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = InsertLiteral(
      field = FIELD_1,
      value = "Hello",
      offset = 0,
      offsetFrom = Some("right")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("Hello", "MathematicalHello")
    }
  }

  test("Choose an offsetFrom value that is not in the set") {
    Given("config")

    val config = ConfigFactory.parseString(s"""
           |  {
           |    type: "insertliteral"
           |    field: "$FIELD_1"
           |    value: "Hello"
           |    offset: "110"
           |    offsetFrom: "Eleventy"
           |  }
      """.stripMargin)

    Given("column to parse")
    val content = Seq(
      Row("Hello"),
      Row("World"),
      Row("I am Calculatron")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = InsertLiteral(
      field = FIELD_1,
      value = "Hello",
      offset = 110,
      offsetFrom = Some("Eleventy")
    ).validate.map(op => op.transform(df))

    dfResult.isLeft shouldBe true

    dfResult.leftMap { error =>
      error.toString() shouldBe "Error in transformation InsertLiteral: The paramater offsetFrom has the invalid value Eleventy. Only the values left and right are allowed for offsetFrom."
    }
  }

  test("Attempt to perform this transformation on a List") {

    val content = Seq(
      Row(Row("hi", "fly", "bye")),
      Row(Row("hi", "why", "bye")),
      Row(Row("hi", "dry", "bye"))
    )
    val df = createDataFrame(content, NestedTwoLevels(1, 3))

    assertThrows[AnalysisException] {
      InsertLiteral(
        field = FIELD_1,
        value = "Hello",
        offset = 1,
        offsetFrom = Some("left")
      ).transform(df)
    }
  }
}
