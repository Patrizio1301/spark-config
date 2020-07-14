package validation

import java.text.SimpleDateFormat

import cats.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import validation.validations.RangeCase
import validation.Validate._
import validation.Validations._
import java.sql.Timestamp

@RunWith(classOf[JUnitRunner])
class RangeTest
    extends InitSparkSessionFunSuite
    with GivenWhenThen
    with Matchers {

  val columnToCheck_Int = Seq(
    Row(1),
    Row(20),
    Row(-20),
    Row(200),
    Row(30)
  )

  test("int min max") {

    val df_Int = createDataFrame(columnToCheck_Int, inputSchema(1, IntegerType))

    val dfResult=RangeCase(FIELD_1, -30, 300).testing(df_Int)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe true)
  }

//  test("int min failures max") {
//
//    val df_Int: DataFrame = createDataFrame(columnToCheck_Int, inputSchema(1, IntegerType))
//
//    val dfResult=RangeCase(FIELD_1, 20, 300).testing(df_Int)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("int min max failures") {
//
//    val df_Int: DataFrame = createDataFrame(columnToCheck_Int, inputSchema(1, IntegerType))
//
//    val dfResult=RangeCase(FIELD_1, -400, 100).testing(df_Int)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }

//  test("int NO max") {
//
//    val df_Int: DataFrame = createDataFrame(columnToCheck_Int, inputSchema(1, IntegerType))
//
//    val dfResult=RangeCase(field = FIELD_1, min = -400).testing(df_Int)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("int NO min") {
//
//    val df_Int: DataFrame = createDataFrame(columnToCheck_Int, inputSchema(1, IntegerType))
//
//    val dfResult=RangeCase(field = FIELD_1, max = 400).testing(df_Int)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  val columnToCheck_Double = Seq(
//    Row(1.1),
//    Row(20.1),
//    Row(-20.1),
//    Row(200.1),
//    Row(30.1)
//  )
//
//  test("double min max") {
//
//    val df_Double: DataFrame =
//      createDataFrame(columnToCheck_Double, inputSchema(1, DoubleType))
//
//    val dfResult=RangeCase(FIELD_1, -20.5,300.0).testing(df_Double)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("double min failures max") {
//
//    val df_Double: DataFrame =
//      createDataFrame(columnToCheck_Double, inputSchema(1, DoubleType))
//
//    val dfResult=RangeCase(FIELD_1, 20.5,300.0).testing(df_Double)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("double min max failures") {
//
//    val df_Double: DataFrame =
//      createDataFrame(columnToCheck_Double, inputSchema(1, DoubleType))
//
//    val dfResult=RangeCase(FIELD_1, -20.5,-300.0).testing(df_Double)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("double NO max") {
//
//    val df_Double: DataFrame =
//      createDataFrame(columnToCheck_Double, inputSchema(1, DoubleType))
//
//    val dfResult=RangeCase(field= FIELD_1, min= -20.5).testing(df_Double)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("double NO min") {
//
//    val df_Double: DataFrame =
//      createDataFrame(columnToCheck_Double, inputSchema(1, DoubleType))
//
//    val dfResult=RangeCase(field = FIELD_1, max = -300.0).testing(df_Double)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  val columnToCheck_Float = Seq(
//    Row(1.1F),
//    Row(20.1F),
//    Row(-20.1F),
//    Row(200.1F),
//    Row(30.1F)
//  )
//
//  test("float min max") {
//
//    val df_Float: DataFrame =
//      createDataFrame(columnToCheck_Float, inputSchema(1, FloatType))
//
//    val dfResult=RangeCase(FIELD_1,-20.5F, 300.0F).testing(df_Float)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("float min failures max") {
//
//    val df_Float: DataFrame =
//      createDataFrame(columnToCheck_Float, inputSchema(1, FloatType))
//
//    val dfResult=RangeCase(FIELD_1,20.5F, 300.0F).testing(df_Float)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("float min max failures") {
//
//    val df_Float: DataFrame =
//      createDataFrame(columnToCheck_Float, inputSchema(1, FloatType))
//
//    val dfResult=RangeCase(FIELD_1,-20.5F, 150.0F).testing(df_Float)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("float NO max") {
//
//    val df_Float: DataFrame =
//      createDataFrame(columnToCheck_Float, inputSchema(1, FloatType))
//
//    val dfResult=RangeCase(FIELD_1,min= -20.5F).testing(df_Float)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("float NO min") {
//
//    val df_Float: DataFrame =
//      createDataFrame(columnToCheck_Float, inputSchema(1, FloatType))
//
//    val dfResult=RangeCase(FIELD_1,max= 250.0F).testing(df_Float)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("float min double max") {
//
//    val df_Float: DataFrame =
//      createDataFrame(columnToCheck_Float, inputSchema(1, FloatType))
//
//    val dfResult=RangeCase(FIELD_1,-20.5F, 300.4D).testing(df_Float)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("int min double max column float") {
//
//    val df_Float: DataFrame =
//      createDataFrame(columnToCheck_Float, inputSchema(1, FloatType))
//
//    val dfResult=RangeCase(FIELD_1,-20, 300.6D).testing(df_Float)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }

  val DATE_FORMAT = "dd-MM-yyyy"
  val dateFormat = new SimpleDateFormat(DATE_FORMAT)
  val columnToCheck_Timestamp = Seq(
    Row(new Timestamp(dateFormat.parse("13-01-1991").getTime)),
    Row(new Timestamp(dateFormat.parse("13-02-1991").getTime)),
    Row(new Timestamp(dateFormat.parse("13-03-1991").getTime)),
    Row(new Timestamp(dateFormat.parse("13-04-1991").getTime)),
    Row(new Timestamp(dateFormat.parse("13-05-1991").getTime))
  )


//  test("timestamp min timestamp max") {
//    val df: DataFrame =
//      createDataFrame(columnToCheck_Timestamp, inputSchema(1, TimestampType))
//
//    val dfResult=RangeCase(FIELD_1,
//      new Timestamp(dateFormat.parse("12-02-1990").getTime),
//      new Timestamp(dateFormat.parse("13-06-1991").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("timestamp min failures timestamp max") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_Timestamp, inputSchema(1, TimestampType))
//
//    val dfResult=RangeCase(FIELD_1,
//      new Timestamp(dateFormat.parse("12-02-1991").getTime),
//      new Timestamp(dateFormat.parse("13-06-1991").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("timestamp min timestamp max failures") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_Timestamp, inputSchema(1, TimestampType))
//
//    val dfResult=RangeCase(FIELD_1,
//      new Timestamp(dateFormat.parse("12-02-1990").getTime),
//      new Timestamp(dateFormat.parse("13-06-1989").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("timestamp NO max") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_Timestamp, inputSchema(1, TimestampType))
//
//    val dfResult=RangeCase(FIELD_1,
//      min=new Timestamp(dateFormat.parse("12-02-1990").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("timestamp NO min") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_Timestamp, inputSchema(1, TimestampType))
//
//    val dfResult=RangeCase(FIELD_1,
//      max= new Timestamp(dateFormat.parse("13-06-1989").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  val columnToCheck_sqlDate = Seq(
//    Row(new java.sql.Date(dateFormat.parse("13-01-1991").getTime)),
//    Row(new java.sql.Date(dateFormat.parse("13-02-1991").getTime)),
//    Row(new java.sql.Date(dateFormat.parse("13-03-1991").getTime)),
//    Row(new java.sql.Date(dateFormat.parse("13-04-1991").getTime)),
//    Row(new java.sql.Date(dateFormat.parse("13-05-1991").getTime))
//  )
//
//  test("sql date min sql date max") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_sqlDate, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(FIELD_1,
//      new java.sql.Date(dateFormat.parse("12-02-1990").getTime),
//      new java.sql.Date(dateFormat.parse("13-06-1999").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("sql date min failures sql date max") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_sqlDate, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(FIELD_1,
//      new java.sql.Date(dateFormat.parse("12-02-1992").getTime),
//      new java.sql.Date(dateFormat.parse("13-06-1999").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("sql date min sql date max failures") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_sqlDate, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(FIELD_1,
//      new java.sql.Date(dateFormat.parse("12-02-1990").getTime),
//      new java.sql.Date(dateFormat.parse("13-06-1989").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("sql date NO max") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_sqlDate, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(FIELD_1,
//      min=new java.sql.Date(dateFormat.parse("12-02-1990").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("sql date NO min") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_sqlDate, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(FIELD_1,
//      max= new java.sql.Date(dateFormat.parse("13-06-1995").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("util date min util date max") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_sqlDate, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(FIELD_1,
//      new java.util.Date(dateFormat.parse("12-02-1990").getTime),
//      new java.util.Date(dateFormat.parse("13-06-1999").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("util date min failures util date max") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_sqlDate, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(FIELD_1,
//      new java.util.Date(dateFormat.parse("12-02-1992").getTime),
//      new java.util.Date(dateFormat.parse("13-06-1999").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("util date min util date max failures") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_sqlDate, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(FIELD_1,
//      new java.util.Date(dateFormat.parse("12-02-1990").getTime),
//      new java.util.Date(dateFormat.parse("13-06-1989").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
//
//  test("util date NO max") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_sqlDate, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(field=FIELD_1,
//      min=new java.util.Date(dateFormat.parse("12-02-1990").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }
//
//  test("util date NO min") {
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck_sqlDate, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(field=FIELD_1,
//      max=new java.util.Date(dateFormat.parse("12-02-1995").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe true)
//  }

  test("Error test: min int max date") {

    val df_Int = createDataFrame(columnToCheck_Int, inputSchema(1, IntegerType))

    val  max=new java.util.Date(dateFormat.parse("12-02-1995").getTime)
    val dfResult=RangeCase(FIELD_1, 1, max).testing(df_Int)

    dfResult.isRight shouldBe false
    dfResult.map(result => result shouldBe true)
  }

}