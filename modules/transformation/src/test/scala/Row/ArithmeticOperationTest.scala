package Row

import cats.implicits._
import transformation.transformations.Arithmeticoperation
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import transformation.Transform._
import transformation.Transformations._

@RunWith(classOf[JUnitRunner])
class ArithmeticOperationTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  val FLOATING_NUMBER_PRECISION = 0.00001
  val fieldResult               = "result"

  test("Add two columns together properly") {

    val content = Seq(
      Row(2, 2),
      Row(-10465, 12),
      Row(65, 0),
      Row(65, -0),
      Row(-25, -25),
      Row(0, 0),
      Row(111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 7
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(4, -10453, 65, 65, -50, 0, 222444666)
    }
  }

  test("Subtract two columns from one another properly") {

    val content = Seq(
      Row(2, 2),
      Row(-10465, 12),
      Row(65, 0),
      Row(65, -0),
      Row(-25, -25),
      Row(0, 0),
      Row(111222333, -111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("-")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 7
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(0, -10477, 65, 65, 0, 0, 222444666)
    }
  }

  test("Multiply two columns together properly") {

    val content = Seq(
      Row(2, 2),
      Row(-10465, 12),
      Row(65, 0),
      Row(65, -0),
      Row(-25, -25),
      Row(0, 0),
      Row(123, 123)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("*")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 7
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(4, -125580, 0, 0, 625, 0, 15129)
    }
  }

  test("Divide two columns properly") {
    val content = Seq(
      Row(2, 2),
      Row(-10465, 12),
      Row(65, 0),
      Row(65, -0),
      Row(-25, -25),
      Row(111222333, 9)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("/")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 6
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(1.0, -872.0833333333334, null, 12358037)
    }
  }

  test("Take the Modulus of two columns properly") {

    val content = Seq(
      Row(2, 2),
      Row(-10465, 12),
      Row(0, 65),
      Row(65, 100),
      Row(-25, -13),
      Row(111222333, 10)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("%")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 6
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(0, -1, 0, 65, -12, 3)
    }
  }

  test("Take the power of two columns properly") {

    val content = Seq(
      Row(2, 2),
      Row(10, -2),
      Row(0, 65),
      Row(65, 0),
      Row(-2, 5),
      Row(-1, 6),
      Row(10, 10)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("^")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 7
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(4, .01, 0, 1, -32, 1, 10000000000.0)
    }
  }

  test("Add three columns together properly") {

    val content = Seq(
      Row(2, 2, 2),
      Row(59, 3, -20),
      Row(0, 0, 0),
      Row(111222333, 111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2, FIELD_3),
      field = fieldResult,
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 4
      getColumnAsSet(df, fieldResult) shouldBe Set(6, 42, 0, 333666999)
    }
  }

  test("Subtract three columns from one another properly") {

    val content = Seq(
      Row(2, 2, 2),
      Row(59, -3, 20),
      Row(0, 0, 0),
      Row(111222333, 111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2, FIELD_3),
      field = fieldResult,
      operators = Seq("-")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 4
      getColumnAsSet(df, fieldResult) shouldBe Set(-2, 42, 0, -111222333)
    }
  }

  test("Multiply three columns together properly") {

    val content = Seq(
      Row(2, -2, -2),
      Row(-3, -7, -2),
      Row(0, 0, 0),
      Row(111222333, 111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2, FIELD_3),
      field = fieldResult,
      operators = Seq("*")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 4
      getColumnAsSet(df, fieldResult) shouldBe Set(8, -42, 0, 1928765605)
    }
  }

  test("Divide three columns properly") {

    val content = Seq(
      Row(8, 2, 2),
      Row(336, 4, 2),
      Row(0, 65, 12),
      Row(111222333, 9, 19)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2, FIELD_3),
      field = fieldResult,
      operators = Seq("/")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 4
      getColumnAsSet(df, fieldResult) shouldBe Set(2.0, 42.0, 0.0, 650423.0)
    }
  }

  test("Take the Modulus of three columns properly") {

    val content = Seq(
      Row(2, 2, 2),
      Row(391, 100, 49),
      Row(0, 2, 14),
      Row(111222333, 100, 2)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2, FIELD_3),
      field = fieldResult,
      operators = Seq("%")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 4
      getColumnAsSet(df, fieldResult) shouldBe Set(0, 42, 0, 1)
    }
  }

  test("Take the power of three columns properly") {

    val content = Seq(
      Row(2, 2, 2),
      Row(-1, 3, 3),
      Row(0, 0, 4),
      Row(-2, 3, 4)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2, FIELD_3),
      field = fieldResult,
      operators = Seq("^")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 4
      getColumnAsSet(df, fieldResult) shouldBe Set(16, -1, 1, 4096)
    }
  }

  test("Add non-integer columns together properly") {

    val content = Seq(
      Row(2.2, 2.2),
      Row(-104.65, 1.2),
      Row(0.0, 0.0),
      Row(111.222333, 111.222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, DoubleType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 3
      df.collect
        .map(_.getAs[Double](s"$fieldResult"))
        .toList
        .zip(List(4.4, -103.45, 0.0, 222.444666))
        .foreach(testValues => {
          assert(testValues._1 ~= testValues._2)
        })
    }
  }

  test("Subtract non-integer columns from one another properly") {

    val content = Seq(
      Row(4.2, 2.4),
      Row(-104.65, 1.2),
      Row(0.0, 0.0),
      Row(111.222333, -111.222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, DoubleType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("-")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 3
      df.collect
        .map(_.getAs[Double](s"$fieldResult"))
        .toList
        .zip(List(1.8, -105.85, 0.0, 222.444666))
        .foreach(testValues => {
          assert(testValues._1 ~= testValues._2)
        })
    }
  }

  test("Multiply non-integer columns together properly") {

    val content = Seq(
      Row(2.0, 2.0),
      Row(-10.6, 2.5),
      Row(0.0, 0.0),
      Row(12.3, 12.3)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, DoubleType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("*")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 3
      df.collect
        .map(_.getAs[Double](s"$fieldResult"))
        .toList
        .zip(List(4.0, -26.5, 0.0, 151.29))
        .foreach(testValues => {
          assert(testValues._1 ~= testValues._2)
        })
    }

  }

  test("Divide non-integer columns properly") {

    val content = Seq(
      Row(2.0, 4.0),
      Row(-21.9, 3.0),
      Row(0.0, 65.0),
      Row(111.222333, 9.0)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, DoubleType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("/")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 3
      df.collect
        .map(_.getAs[Double](s"$fieldResult"))
        .toList
        .zip(List(0.5, -7.3, 0.0, 12.358037))
        .foreach(testValues => {
          assert(testValues._1 ~= testValues._2)
        })
    }
  }

  test("Take the Modulus of non-integer columns properly") {

    val content = Seq(
      Row(2.4, 2.2),
      Row(-104.65, 12.0),
      Row(0.0, 65.0),
      Row(111.222333, 10.01)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, DoubleType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("%")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 3
      df.collect
        .map(_.getAs[Double](s"$fieldResult"))
        .toList
        .zip(List(0.2, -8.65, 0.0, 1.112333))
        .foreach(testValues => {
          assert(testValues._1 ~= testValues._2)
        })
    }
  }

  test("Take the power of non-integer columns properly") {

    val content = Seq(
      Row(2.4, 2.0),
      Row(2.0, 10.0),
      Row(4.0, 2.5),
      Row(1.4, 10.1)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, DoubleType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("^")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 3
      df.collect
        .map(_.getAs[Double](s"$fieldResult"))
        .toList
        .zip(List(5.76, 1024.0, 32.0, 29.9152860795))
        .foreach(testValues => {
          assert(testValues._1 ~= testValues._2)
        })
    }
  }

  test("Add two columns together with a literal integer value instead of a column") {

    val content = Seq(
      Row(2, 2),
      Row(-10465, 12),
      Row(65, 0),
      Row(-0, 65),
      Row(-25, -25),
      Row(0, 0),
      Row(111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, "2"),
      field = fieldResult,
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 7
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(4, -10463, 67, 2, -23, 2, 111222335)
    }

  }

  test("Add two columns together with a literal double value instead of a column") {

    val content = Seq(
      Row(2, 2),
      Row(-10465, 12),
      Row(65, 0),
      Row(-0, 65),
      Row(-25, -25),
      Row(0, 0),
      Row(111222333, 111222333)
    )
    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, "25.4"),
      field = fieldResult,
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 7
      df.columns.length shouldBe 3
      df.collect
        .map(_.getAs[Double](s"$fieldResult"))
        .toList
        .zip(List(27.4, -10439.6, 90.4, 25.4, 0.4, 25.4, 111222358.4))
        .foreach(testValues => {
          assert(testValues._1 ~= testValues._2)
        })
    }

  }

  test("Subtract three columns with literal integer values instead of columns") {

    val content = Seq(
      Row(2, 2),
      Row(-10465, 12),
      Row(65, 0),
      Row(-0, 65),
      Row(-25, -25),
      Row(0, 0),
      Row(111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq("-1", FIELD_1, "2"),
      field = fieldResult,
      operators = Seq("-")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 7
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(-5, 10462, -68, -3, 22, -3, -111222336)
    }

  }

  test("Multiply two columns with a negative <1 decimal value instead of a column") {

    val content = Seq(
      Row(2, 2),
      Row(-10464, 12),
      Row(60, 0),
      Row(-0, 65),
      Row(-25, -25),
      Row(0, 0),
      Row(111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, "-.5"),
      field = fieldResult,
      operators = Seq("*")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 7
      df.columns.length shouldBe 3
      df.collect
        .map(_.getAs[Double](s"$fieldResult"))
        .toList
        .zip(List(-1, 5232, -30, 0, 12.5, 0, -55611166.5))
        .foreach(testValues => {
          assert(testValues._1 ~= testValues._2)
        })
    }
  }

  test("Add two columns where one has an integer name properly") {

    val content = Seq(
      Row(2, 2),
      Row(-10465, 12),
      Row(65, 0),
      Row(65, -0),
      Row(-25, -25),
      Row(0, 0),
      Row(111222333, 111222333)
    )

    val df: DataFrame =
      createDataFrame(content, simpleSchema(Seq(IntegerType), Seq(FIELD_1, "2")))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, "2"),
      field = fieldResult,
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 7
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(4, -10453, 65, 65, -50, 0, 222444666)
    }
  }

  test("Perform operations on a field that already exists that is being used in the transformation") {

    val content = Seq(
      Row(2, 2, 2),
      Row(59, 3, -20),
      Row(0, 0, 0),
      Row(111222333, 111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2, FIELD_3),
      field = FIELD_2,
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 3
      getColumnAsSet(df, FIELD_2) shouldBe Set(6, 42, 0, 333666999)
    }

  }

  test("Perform two different operations on a set of three columns") {

    val content = Seq(
      Row(2, 2, 2),
      Row(59, 3, -20),
      Row(0, 0, 0),
      Row(111222333, 111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2, FIELD_3),
      field = fieldResult,
      operators = Seq("+", "-")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 4
      getColumnAsSet(df, fieldResult) shouldBe Set(2, 82, 0, 111222333)
    }
  }

  test("Perform five different operations on a set of three columns and three literals") {

    val content = Seq(
      Row(2, 2, 2),
      Row(59, 3, -20),
      Row(0, 0, 0),
      Row(111222333, 111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq("2", FIELD_1, FIELD_2, "5", FIELD_3, "111.11"),
      field = fieldResult,
      operators = Seq("*", "-", "/", "-", "+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 4
      df.collect
        .map(_.getAs[Double](s"$fieldResult"))
        .toList
        .zip(List(109.51, 154.11, 111.11, -88977755.29))
        .foreach(testValues => {
          assert(testValues._1 ~= testValues._2)
        })
    }
  }

  test(
    "Perform three different operations on a set of five literals and a column while padding the final operator") {

    val content = Seq(
      Row(2, 2, 2),
      Row(59, 3, -20),
      Row(0, 0, 0),
      Row(111222333, 111222333, 111222333)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq("2", "5", "2", "5", "3", FIELD_1),
      field = fieldResult,
      operators = Seq("*", "-", "+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 4
      df.columns.length shouldBe 4
      getColumnAsSet(df, fieldResult) shouldBe Set(18, 75, 16, 111222349)
    }
  }

  test("Try to add null values") {

    val content = Seq(
      Row("2", ""),
      Row("", "16"),
      Row("", "")
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 3
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(null)
    //This results from the way Spark interprets nulls as zeroes when collecting and showing the data
    }
  }

  test("Try to add string values") {

    val content = Seq(
      Row("Spaghetti", "Pasta"),
      Row("Fettucine", "Alfredo Sauce"),
      Row("Ramen", "Pork Buns")
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 3
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(null)
    //This results from the way Spark interprets nulls as zeroes when collecting and showing the data
    }
  }

  test("Try to divide string values") {

    val content = Seq(
      Row("two", "four"),
      Row("sixteen", "eight"),
      Row("one", "one")
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 3
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(null)
    }
  }

  test("Try to divide null values and divide by 0") {

    val content = Seq(
      Row("", ""),
      Row("", "15"),
      Row("42", ""),
      Row("12", "0"),
      Row("0", "0")
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("/")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      df.count() shouldBe 5
      df.columns.length shouldBe 3
      getColumnAsSet(df, fieldResult) shouldBe Set(null)
    //This results from the way Spark interprets nulls as zeroes when collecting and showing the data
    }
  }

  test("Try to add a single column as a new column") {

    val content = Seq(
      Row(1),
      Row(2),
      Row(3)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1),
      field = fieldResult,
      operators = Seq("+")
    ).validate
      .map(dataFrame => dataFrame.transform(df))

    result.isLeft shouldBe true

    result.leftMap(
      error =>
        error.toString shouldBe "Error in transformation ArithmeticOperation: " +
          "The parameter valuesToOperateOn is an empty list. These operations " +
          "cannot be performed on one or zero columns." +
        " Add two or more column names into valuesToOperateOn: [<column1>, <column2>]")
  }

  test("Try to add a single column as itself") {

    val content = Seq(
      Row(1),
      Row(2),
      Row(3)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1),
      field = FIELD_1,
      operators = Seq("+")
    ).validate
      .map(result => result.transform(df))

    result.isLeft shouldBe true

    result.leftMap { error =>
      error.toString shouldBe
        "Error in transformation ArithmeticOperation: " +
          "The parameter valuesToOperateOn is an empty list. " +
          "These operations cannot be performed on one or zero columns." +
          " Add two or more column names into valuesToOperateOn: [<column1>, <column2>]"
    }
  }

  test("Try to add no columns") {

    val content = Seq(
      Row(1),
      Row(2),
      Row(3)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(),
      field = fieldResult,
      operators = Seq("+")
    ).validate
      .map(result => result.transform(df))

    result.isLeft shouldBe true

    result.leftMap { error =>
      error.toString shouldBe "Error in transformation ArithmeticOperation: " +
        "The parameter valuesToOperateOn is an empty list. " +
        "These operations cannot be performed on one or zero columns." +
        " Add two or more column names into valuesToOperateOn: [<column1>, <column2>]"
    }
  }

  test("Check that a nonsense separator throws an error") {

    val content = Seq(
      Row(2, 2),
      Row(-10465, 12),
      Row(65, 0),
      Row(65, 10)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, IntegerType))

    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(FIELD_1, FIELD_2),
      field = fieldResult,
      operators = Seq("T")
    ).transform(df)

    result.isLeft shouldBe true

    result.leftMap { error =>
      error.toString shouldBe "Error in transformation ArithmeticOperation: " +
        "Operation unspecified or not allowed. Available operations are +, -, *, /, %, ^"
    }
  }

  test("Check that a non-existent column addition throws an error") {

    val content = Seq(
      Row(1),
      Row(2),
      Row(3)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, IntegerType))
    assertThrows[AnalysisException] {
      val result = Arithmeticoperation(
        valuesToOperateOn = Seq(FIELD_1, "columnQ"),
        field = fieldResult,
        operators = Seq("+")
      ).transform(df)

      result.isLeft shouldBe true

      result.leftMap { error =>
        error.toString shouldBe "Error in transformation ArithmeticOperation: " +
          "Operation unspecified or not allowed. Available operations are +, -, *, /, %, ^"
      }

    }
  }

  test("with nested structure") {

    val content = Seq(Row(Row(Row("dd"), 1, 2), "BB", "AA"))
    val df =
      createDataFrame(content, nestedSchema(Seq(StringType, IntegerType, StringType), Seq(3, 3, 1)))
    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(s"$FIELD_1.$FIELD_2", s"$FIELD_1.$FIELD_3"),
      field = s"$FIELD_1.$FIELD_4",
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map(df => getColumnAsSet(df, s"$FIELD_1.$FIELD_4") shouldBe Set(3))
  }

  test("with nested structure nivel 2") {

    val content = Seq(Row(Row(Row(4), 1, 2), "BB", "AA"))
    val df =
      createDataFrame(content,
                      nestedSchema(Seq(StringType, IntegerType, IntegerType), Seq(3, 3, 1)))
    val result = Arithmeticoperation(
      valuesToOperateOn = Seq(s"$FIELD_1.$FIELD_1.$FIELD_1", s"$FIELD_1.$FIELD_3"),
      field = s"$FIELD_1.$FIELD_4",
      operators = Seq("+")
    ).transform(df)

    result.isRight shouldBe true

    result.map(df => getColumnAsSet(df, s"$FIELD_1.$FIELD_4") shouldBe Set(6))
  }

  implicit class ApproximateDouble(d: Double) {
    def ~=(x: Double, precision: Double = FLOATING_NUMBER_PRECISION): Boolean = {
      if ((d - x).abs < precision) true else false
    }
  }
}
