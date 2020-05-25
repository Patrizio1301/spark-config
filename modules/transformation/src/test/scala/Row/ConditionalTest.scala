package Row

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import transformation.transformations.{Conditional, Expression}
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import transformation.Transform._
import transformation.Transformations._

@RunWith(classOf[JUnitRunner])
class ConditionalTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("check correct cast from string to decimal") {

    val content = Seq(
      Row(Row("hpla", "huhu"), "hulo"),
      Row(Row("hpla", "huhu"), "hulu")
    )

    val df: DataFrame = createDataFrame(content, NestedTwoLevels(2, 2))

    val result = Conditional(
      field = FIELD_3,
      default = "default",
      dataType = "string",
      expressions = Seq(
        Expression(
          condition = "B='hulu'",
          field = Some("A.A")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_3) shouldBe List("default", "hpla")
    }
  }

  test("check that condition works with more than one condition") {
    val content = Seq(
      Row(Row("hpla", "huhu"), "hulo"),
      Row(Row("hpla", "huhu"), "hulu")
    )

    val df: DataFrame = createDataFrame(content, NestedTwoLevels(2, 2))

    val result = Conditional(
      field = FIELD_3,
      default = "default",
      dataType = "string",
      expressions = Seq(
        Expression(
          condition = "B='hulu'",
          field = Some("A.A")
        ),
        Expression(
          condition = "B='hulo'",
          field = Some("A.B")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_3) shouldBe List("huhu", "hpla")
    }

  }

  test("check that condition works with AND") {
    val content = Seq(
      Row(Row("hola", "que"), "tal"),
      Row(Row("adios", "que"), "tal"),
      Row(Row("muy", "bien"), "tu")
    )

    val df: DataFrame = createDataFrame(content, NestedTwoLevels(2, 2))

    val result = Conditional(
      field = FIELD_3,
      default = "default",
      dataType = "string",
      expressions = Seq(
        Expression(
          condition = "B='tal' AND A.A='hola'",
          field = Some("A.A")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_3) shouldBe List("hola", "default", "default")
    }
  }

  test("check that condition works with OR") {
    val content = Seq(
      Row(Row("adios", "que"), "tal"),
      Row(Row("hola", "que"), "tal"),
      Row(Row("hola", "que"), "yo"),
      Row(Row("muy", "bien"), "tu")
    )

    val df: DataFrame = createDataFrame(content, NestedTwoLevels(2, 2))

    val result = Conditional(
      field = FIELD_3,
      default = "default",
      dataType = "string",
      expressions = Seq(
        Expression(
          condition = "B='tal' OR A.A='hola'",
          field = Some("A.A")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_3) shouldBe List("adios", "hola", "hola", "default")
    }
  }

  test("check that condition works with sumation") {

    val content = Seq(
      Row(Row("adios", "que"), "tal"),
      Row(Row("hola", "que"), "tal"),
      Row(Row("hola", "que"), "yo"),
      Row(Row("muy", "bien"), "tu")
    )

    val df: DataFrame = createDataFrame(content, NestedTwoLevels(2, 2))

    val result = Conditional(
      field = FIELD_3,
      default = "default",
      dataType = "string",
      expressions = Seq(
        Expression(
          condition = "B='tal' OR A.A='hola'",
          field = Some("A.A")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_3) shouldBe List("adios", "hola", "hola", "default")
    }
  }

  test("check that condition works with numerical sums and boolean") {

    val content = Seq(
      Row(Row(1, 2), -3),
      Row(Row(4, 1), -5),
      Row(Row(4, 5), -9),
      Row(Row(2, 2), -5)
    )

    val df: DataFrame = createDataFrame(content, NestedTwoLevels(2, 2))

    val result = Conditional(
      field = FIELD_3,
      default = "false",
      dataType = "boolean",
      expressions = Seq(
        Expression(
          condition = "A.A+A.B+B=0",
          value = Some("true"),
          field = Some("A.A")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_3) shouldBe List(true, true, true, false)
    }
  }

  test("check that condition works with numerical sums and integers") {

    val content = Seq(
      Row(Row(1, 2), -3),
      Row(Row(4, 1), -5),
      Row(Row(4, 5), -9),
      Row(Row(2, 2), -5)
    )

    val df: DataFrame = createDataFrame(content, NestedTwoLevels(2, 2))

    val result = Conditional(
      field = FIELD_3,
      default = "0",
      dataType = "integer",
      expressions = Seq(
        Expression(
          condition = "A.A+A.B+B==0",
          value = Some("1"),
          field = Some("A.A")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_3) shouldBe List(1, 1, 1, 0)
    }
  }

  test("check that condition works with numerical divisions and integers") {

    val content = Seq(
      Row(1, 1),
      Row(2, 2),
      Row(3, 4),
      Row(3, 6)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, IntegerType))

    val result = Conditional(
      field = FIELD_3,
      default = "0",
      dataType = "integer",
      expressions = Seq(
        Expression(
          condition = "A/B==1",
          value = Some("1")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_3) shouldBe List(1, 1, 0, 0)
    }
  }

  test("check that condition works with numerical multiplication and integers") {

    val content = Seq(
      Row(20, 1, 1),
      Row(2, 2, 5),
      Row(10, 2, 1),
      Row(3, 6, 5)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Conditional(
      field = FIELD_4,
      default = "0",
      dataType = "integer",
      expressions = Seq(
        Expression(
          condition = "A*B*C==20",
          value = Some("1")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_4) shouldBe List(1, 1, 1, 0)
    }
  }

  test("check that condition works with numerical multiplication with nulls") {

    val content = Seq(
      Row(20, null, 1),
      Row(2, 2, 5),
      Row(10, null, 1),
      Row(3, 6, 5)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(3, IntegerType))

    val result = Conditional(
      field = FIELD_4,
      default = "0",
      dataType = "integer",
      expressions = Seq(
        Expression(
          condition = "A*B*C==20",
          value = Some("1")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_4) shouldBe List(0, 1, 0, 0)
    }
  }

  test("check that condition works with numerical multiplication with decimals") {

    val content = Seq(
      Row(new java.math.BigDecimal("20.0"), new java.math.BigDecimal("1.0")),
      Row(new java.math.BigDecimal("20.0"), new java.math.BigDecimal("1.0")),
      Row(new java.math.BigDecimal("20.0"), new java.math.BigDecimal("20.0")),
      Row(new java.math.BigDecimal("20.0"), new java.math.BigDecimal("20.0"))
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, DecimalType(20, 2)))

    val result = Conditional(
      field = FIELD_3,
      default = "0.0",
      dataType = "decimal(3,2)",
      expressions = Seq(
        Expression(
          condition = "A*B==20",
          value = Some("1.0")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_3) shouldBe List(
        new java.math.BigDecimal("1.00"),
        new java.math.BigDecimal("1.00"),
        new java.math.BigDecimal("0.00"),
        new java.math.BigDecimal("0.00")
      )
    }
  }

  test("check that condition works with date inequalities and boolean") {

    val sdf1 = new SimpleDateFormat("MM-dd-yyyy")
    val content = Seq(
      Row(new Date(sdf1.parse("07-17-2018").getTime)),
      Row(new Date(sdf1.parse("07-17-2019").getTime)),
      Row(new Date(sdf1.parse("07-17-2016").getTime)),
      Row(new Date(sdf1.parse("07-17-2015").getTime))
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, DateType))

    val result = Conditional(
      field = FIELD_2,
      default = "false",
      dataType = "boolean",
      expressions = Seq(
        Expression(
          condition = " A > '2017-07-17'",
          value = Some("true")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_2) shouldBe List(true, true, false, false)
    }
  }

  test("check that condition works with timestamp inequalities and boolean") {
    val sdf1 = new SimpleDateFormat("MM-dd-yyyy")
    val content = Seq(
      Row(new Date(sdf1.parse("07-17-2018").getTime)),
      Row(new Date(sdf1.parse("07-17-2019").getTime)),
      Row(new Date(sdf1.parse("07-17-2016").getTime)),
      Row(new Date(sdf1.parse("07-17-2015").getTime))
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, DateType))

    val result = Conditional(
      field = FIELD_2,
      default = "2007-04-01",
      dataType = "date",
      expressions = Seq(
        Expression(
          condition = " A > '2017-07-17'",
          value = Some("2017-04-01")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_2) shouldBe List(
        new Date(sdf1.parse("04-01-2017").getTime),
        new Date(sdf1.parse("04-01-2017").getTime),
        new Date(sdf1.parse("04-01-2007").getTime),
        new Date(sdf1.parse("04-01-2007").getTime)
      )
    }
  }

  test("check that condition works with timestamp inequalities and new boolean column") {

    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val content = Seq(
      Row(new Timestamp(sdf1.parse("2013-01-01 11:01:11").getTime)),
      Row(new Timestamp(sdf1.parse("2013-02-02 11:01:22").getTime)),
      Row(new Timestamp(sdf1.parse("1999-01-01 11:01:33").getTime))
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, TimestampType))

    val result = Conditional(
      field = FIELD_2,
      default = "false",
      dataType = "boolean",
      expressions = Seq(
        Expression(
          condition = " A >= '2013-01-01 11:01:11'",
          value = Some("true")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_2) shouldBe List(true, true, false)
    }
  }

  test("check that condition works with timestamp inequalities and new timestamp column") {
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val content = Seq(
      Row(new Timestamp(sdf1.parse("2013-01-01 11:01:11").getTime)),
      Row(new Timestamp(sdf1.parse("2013-02-02 11:01:22").getTime)),
      Row(new Timestamp(sdf1.parse("1999-01-01 11:01:33").getTime))
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, TimestampType))

    val result = Conditional(
      field = FIELD_2,
      default = "2001-01-01 11:01:11",
      dataType = "timestamp",
      expressions = Seq(
        Expression(
          condition = " A >= '2013-01-01 11:01:11'",
          value = Some("2013-01-01 11:01:11")
        )
      )
    ).transform(df)

    result.isRight shouldBe true
    result.map { df =>
      getColumnAsList(df, FIELD_2) shouldBe List(
        new Timestamp(sdf1.parse("2013-01-01 11:01:11").getTime),
        new Timestamp(sdf1.parse("2013-01-01 11:01:11").getTime),
        new Timestamp(sdf1.parse("2001-01-01 11:01:11").getTime)
      )
    }
  }
}
