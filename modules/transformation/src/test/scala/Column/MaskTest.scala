package Column

import transformation.transformations.Mask
import utils.test._
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import transformation.Transformations._
import transformation.Transform._
import cats.implicits._

@RunWith(classOf[JUnitRunner])
class MaskTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  val transformation = (df: DataFrame, dataType: String) => Mask(FIELD_1, dataType).transform(df)

  test("mask integer field") {

    val content = Seq(
      Row(1),
      Row(2),
      Row(0)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, IntegerType))

    transformation(df, "int").map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(0)
    }
  }

  test("mask double field") {
    val content = Seq(
      Row(1D),
      Row(2D),
      Row(0D)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, DoubleType))

    transformation(df, "double").map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(0D)
    }
  }

  test("mask long field") {
    val content = Seq(
      Row(1L),
      Row(2L),
      Row(0L)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, LongType))

    transformation(df, "long").map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(0)
    }
  }

  test("mask float field") {
    val content = Seq(
      Row(1F),
      Row(2F),
      Row(0F)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, FloatType))

    transformation(df, "float").map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set(0F)
    }
  }

 /*
 test("mask date field") {
    val content = Seq(
      Row(new Date(0)),
      Row(new Date(1)),
      Row(new Date(2))
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, DateType))

    //TODO: IMPROVE THIS TEST
    transformation(df, "date").map { result =>
      getColumnAsList(result, FIELD_1)(0).toString shouldBe new Date(0).toString
    }
  }
  */

  test("mask other field") {
    val content = Seq(
      Row(true),
      Row(false),
      Row(false)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, BooleanType))

    transformation(df, "boolean").map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("XX")
    }
  }
}
