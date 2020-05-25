package Row

import java.sql.Date
import java.text.SimpleDateFormat

import transformation.transformations.Autocast
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
class AutoCastTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("check correct cast from string to decimal") {

    val content = Seq(
      Row("3.4"),
      Row("4.5"),
      Row("5.8")
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, StringType))

    val result = Autocast(
      fromType = "string",
      toType = "decimal(2,1)"
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.collect.toList shouldBe List(
        Row(new java.math.BigDecimal("3.4")),
        Row(new java.math.BigDecimal("4.5")),
        Row(new java.math.BigDecimal("5.8"))
      )
    }
  }

  test("check correct cast from string to date") {

    val content = Seq(
      Row("2017-07-17"),
      Row("2017-07-18"),
      Row("2017-07-16")
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, StringType))

    val result = Autocast(
      fromType = "string",
      toType = "date",
      format = Some("yyyy-MM-dd")
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
      result.collect().toList shouldBe List(
        Row(new Date(sdf1.parse("2017-07-17").getTime)),
        Row(new Date(sdf1.parse("2017-07-18").getTime)),
        Row(new Date(sdf1.parse("2017-07-16").getTime))
      )
    }
  }

//    test("check correct cast from date to string") {
//
//      val sdf1 = new SimpleDateFormat("MM-yyyy-dd")
//      val content: Seq[Row] = Seq(
//        Row(new Date(sdf1.parse("07-2017-17").getTime)),
//        Row(new Date(sdf1.parse("07-2017-18").getTime)),
//        Row(new Date(sdf1.parse("07-2017-16").getTime))
//      )
//
//      val df: DataFrame = createDataFrame(content, inputSchema(1, DateType))
//
//      val result = Autocast(
//        fromType = "date",
//        toType = "string",
//        format = Some("yyyy-MM-dd")
//      ).transform(df)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.collect().toList shouldBe List(
//          Row("07-2017-17"),
//          Row("07-2017-18"),
//          Row("07-2017-16")
//        )
//      }
//    }

  test("check if it changes just the correct column") {

    val content: Seq[Row] = Seq(
      Row("3.4", 12),
      Row("4.5", 14),
      Row("5.8", 23)
    )
    val df: DataFrame = createDataFrame(content, simpleSchema(Seq(StringType, IntegerType)))

    val result = Autocast(
      fromType = "string",
      toType = "decimal(2,1)"
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.collect().toList shouldBe List(
        Row(new java.math.BigDecimal("3.4"), 12),
        Row(new java.math.BigDecimal("4.5"), 14),
        Row(new java.math.BigDecimal("5.8"), 23)
      )
    }
  }

  test("check if it changes several columns") {

    val content: Seq[Row] = Seq(
      Row("3.4", "3.4", 12),
      Row("4.5", "4.5", 14),
      Row("5.8", "5.8", 23)
    )

    val df: DataFrame =
      createDataFrame(content, simpleSchema(Seq(StringType, StringType, IntegerType)))

    val result = Autocast(
      fromType = "string",
      toType = "decimal(2,1)"
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.collect().toList shouldBe List(
        Row(new java.math.BigDecimal("3.4"), new java.math.BigDecimal("3.4"), 12),
        Row(new java.math.BigDecimal("4.5"), new java.math.BigDecimal("4.5"), 14),
        Row(new java.math.BigDecimal("5.8"), new java.math.BigDecimal("5.8"), 23)
      )
    }
  }

  test("check if it changes several columns with exceptions") {

    val content: Seq[Row] = Seq(
      Row("3.4", "3.4", 12),
      Row("4.5", "4.5", 14),
      Row("5.8", "5.8", 23)
    )

    val df: DataFrame =
      createDataFrame(content, simpleSchema(Seq(StringType, StringType, IntegerType)))

    val result = Autocast(
      fromType = "string",
      toType = "decimal(2,1)",
      exceptions = Seq(FIELD_2)
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.collect().toList shouldBe List(
        Row(new java.math.BigDecimal("3.4"), "3.4", 12),
        Row(new java.math.BigDecimal("4.5"), "4.5", 14),
        Row(new java.math.BigDecimal("5.8"), "5.8", 23)
      )
    }
  }

  test("with nested structure") {
    val content = Seq(Row(Row(Row("1.33"), "1.22", "1.44"), "1.22", "1.33"))
    val df: DataFrame =
      createDataFrame(content, nestedSchema(Seq(StringType, StringType, StringType), Seq(3, 3, 1)))

    val result = Autocast(
      fromType = "string",
      toType = "decimal(2,1)"
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.collect().toList shouldBe List(
        Row(
          Row(Row(new java.math.BigDecimal("1.3")),
              new java.math.BigDecimal("1.2"),
              new java.math.BigDecimal("1.4")),
          new java.math.BigDecimal("1.2"),
          new java.math.BigDecimal("1.3")
        ))
    }

  }
}
