package Column

import java.sql.Date
import java.util.Calendar

import transformation.transformations.ExtractInfoFromDate
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
class ExtractInfoFromDateTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  private val calendar = Calendar.getInstance()

  test("extract year") {

    calendar.set(Calendar.YEAR, 2015)
    val day: Date = new Date(calendar.getTime.getTime)

    val content = Seq(
      Row(day),
      Row(day),
      Row(day)
    )

    val df = createDataFrame(content, inputSchema(1, DateType))

    val dfResult = ExtractInfoFromDate(
      field = "year",
      dateField = FIELD_1,
      info = "year"
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      getColumnAsSet(result, "year") shouldBe Set("2015")
      result.collect().length shouldBe 3
    }
  }

  test("extract month") {

    calendar.set(Calendar.MONTH, 2)
    val day: Date = new Date(calendar.getTime.getTime)

    val content = Seq(
      Row(day),
      Row(day),
      Row(day)
    )

    val df = createDataFrame(content, inputSchema(1, DateType))

    val dfResult = ExtractInfoFromDate(
      field = "month",
      dateField = FIELD_1,
      info = "month"
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      getColumnAsSet(result, "month") shouldBe Set("2")
      result.collect().length shouldBe 3
    }
  }

  test("extract day") {

    calendar.set(Calendar.DAY_OF_MONTH, 16)
    val day: Date = new Date(calendar.getTime.getTime)

    val content = Seq(
      Row(day),
      Row(day),
      Row(day)
    )

    val df = createDataFrame(content, inputSchema(1, DateType))

    val dfResult = ExtractInfoFromDate(
      field = "day",
      dateField = FIELD_1,
      info = "day"
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      getColumnAsSet(result, "day") shouldBe Set("16")
      result.collect().length shouldBe 3
    }
  }

  test("support null values") {

    val content = Seq(
      Row(null),
      Row(null),
      Row(null)
    )

    val df = createDataFrame(content, inputSchema(1, DateType))

    val dfResult = ExtractInfoFromDate(
      field = "day",
      dateField = FIELD_1,
      info = "day"
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      getColumnAsSet(result, "day") shouldBe Set(null)
      result.collect().length shouldBe 3
    }
  }
}
