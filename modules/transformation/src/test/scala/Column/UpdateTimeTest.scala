package Column

import java.sql.Timestamp

import transformation.transformations.UpdateTime
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import transformation.Transform._
import transformation.Transformations._

@RunWith(classOf[JUnitRunner])
class UpdateTimeTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("field not exists") {
    val content = Seq(
      Row(3),
      Row(4),
      Row(5),
      Row(6)
    )

    val df = createDataFrame(content, inputSchema(1, IntegerType))

    val dfResult = UpdateTime(
      field = "updateTime"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      val resultListCount: Array[Row] =
        result.select("updateTime").groupBy("updateTime").count().collect
      resultListCount.length shouldBe 1
      resultListCount(0).get(1) shouldBe 4
      resultListCount(0).get(0).isInstanceOf[Timestamp] shouldBe true
    }

  }

  test("field exists") {
    val startTime = java.sql.Timestamp.from(java.time.Instant.now)

    val content = Seq(
      Row(3, startTime),
      Row(4, startTime),
      Row(5, startTime),
      Row(6, startTime)
    )
    val df =
      createDataFrame(content,
                      simpleSchema(Seq(IntegerType, TimestampType), Seq(FIELD_1, "updateTime")))

    val dfResult = UpdateTime(
      field = "updateTime"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      val resultListCount: Array[Row] =
        result.select("updateTime").groupBy("updateTime").count().collect

      resultListCount.length shouldBe 1
      resultListCount(0).get(1) shouldBe 4
      resultListCount(0).get(0).isInstanceOf[Timestamp] shouldBe true
      resultListCount(0).get(0).asInstanceOf[Timestamp].after(startTime) shouldBe true
    }
  }

  test("field exists with strange characters") {
    val startTime = java.sql.Timestamp.from(java.time.Instant.now)

    val content = Seq(
      Row(3, startTime),
      Row(4, startTime),
      Row(5, startTime),
      Row(6, startTime)
    )

    val df =
      createDataFrame(content,
                      simpleSchema(Seq(IntegerType, TimestampType), Seq(FIELD_1, "update(time)")))

    val dfResult = UpdateTime(
      field = "update(time)"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      val resultListCount: Array[Row] =
        result.select("update(time)").groupBy("update(time)").count().collect

      resultListCount.length shouldBe 1
      resultListCount(0).get(1) shouldBe 4
      resultListCount(0).get(0).isInstanceOf[Timestamp] shouldBe true
      resultListCount(0).get(0).asInstanceOf[Timestamp].after(startTime) shouldBe true
    }
  }
}
