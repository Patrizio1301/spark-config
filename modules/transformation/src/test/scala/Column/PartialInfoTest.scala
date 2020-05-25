package Column

import java.sql.Date
import java.util.Calendar

import transformation.transformations.PartialInfo
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
class PartialInfoTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("extract relevance info from string") {
    val content: Seq[Row] = Seq(
      Row("aaaaRelevanceInfoaaaa"),
      Row("BBBBRelevanceInfoBBBB"),
      Row("CC22RelevanceInfoCC11")
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = PartialInfo(
      field = "partialInfoField",
      fieldInfo = FIELD_1,
      start = Some(5),
      length = Some(13)
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, "partialInfoField") shouldBe Set("RelevanceInfo")
      And("Other field should exist")
      assert(
        result
          .select(FIELD_1)
          .collect()
          .map(_.getAs[String](FIELD_1))
          .sameElements(df.collect().map(_.getAs[String](FIELD_1))))
    }
  }

  test("extract relevance info from date") {

    val calendar: Calendar = Calendar.getInstance()
    calendar.set(Calendar.MONTH, Calendar.JULY)
    val dateToTest: Date = new Date(calendar.getTime.getTime)

    val content: Seq[Row] = Seq(
      Row(dateToTest),
      Row(dateToTest),
      Row(dateToTest)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, DateType))

    val dfResult = PartialInfo(
      field = "partialInfoField",
      fieldInfo = FIELD_1,
      start = Some(6),
      length = Some(2)
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, "partialInfoField") shouldBe Set("07")
      assert(
        result
          .select(FIELD_1)
          .collect()
          .map(_.getAs[Date](FIELD_1))
          .sameElements(df.collect().map(_.getAs[Date](FIELD_1))))
    }
  }
}
