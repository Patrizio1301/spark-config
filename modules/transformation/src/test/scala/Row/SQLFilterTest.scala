package Row

import transformation.transformations.SqlFilter
import utils.test._
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{AnalysisException, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import transformation.Transform._
import transformation.Transformations._

@RunWith(classOf[JUnitRunner])
class SQLFilterTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("check correct result with true filter filter") {

      val content = Seq(
        Row("juan", 30),
        Row("ruben", 12),
        Row("juan", 60),
        Row("samuel", 103)
      )

      val df = createDataFrame(content, simpleSchema(Seq(StringType, IntegerType)))

      val result = SqlFilter(filter = "true").transform(df)

      result.isRight shouldBe true

      result.map { df =>
        df.count shouldBe 4
      }
    }

    test("check correct result with complex filter") {

      val content = Seq(
        Row("juan", 30),
        Row("ruben", 12),
        Row("juan", 60),
        Row("samuel", 103)
      )
      val df = createDataFrame(content, simpleSchema(Seq(StringType, IntegerType)))

      val result =
        SqlFilter(filter = s"$FIELD_2 > 40 AND ($FIELD_1 = 'juan' OR $FIELD_1 = 'samuel')")
          .transform(df)

      result.isRight shouldBe true

      result.map { df =>
        df.count shouldBe 2
      }
    }

    test("check malformed filter with invalid column names") {

      val content = Seq(Row("juan", 30), Row("ruben", 12), Row("juan", 60), Row("samuel", 103))
      val df      = createDataFrame(content, simpleSchema(Seq(StringType, IntegerType)))

      intercept[AnalysisException] {
        SqlFilter("C > 40 AND (A = 'juan' OR A = 'samuel')").transform(df)
      }
    }

    test("check malformed filter with invalid operator") {

      val content = Seq(Row("juan", 30), Row("ruben", 12), Row("juan", 60), Row("samuel", 103))

      val df = createDataFrame(content, simpleSchema(Seq(StringType, IntegerType)))

      intercept[ParseException] {
        SqlFilter("weight <!> 40 AND (name = 'juan' OR name = 'samuel')").transform(df)
      }
    }

    test("check filter with not allowed operators") {

      val content = Seq(Row("juan", 30), Row("ruben", 12), Row("juan", 60), Row("samuel", 103))

      val df = createDataFrame(content, simpleSchema(Seq(StringType, IntegerType)))

      intercept[ParseException] {
        SqlFilter(s"$FIELD_2 > 40 AND ($FIELD_1 = 'juan' OR $FIELD_1 = 'samuel') GROUP BY $FIELD_2")
          .transform(df)
      }
    }

  test("check correct result with nested values") {

      val content = Seq(
        Row(Row(Row("row11"), "row12", "row13"), "row14", "row15"),
        Row(Row(Row("row21"), "row22", "row23"), "row24", "row25")
      )

      val df = createDataFrame(content,
                               nestedSchema(Seq(StringType, StringType, StringType), Seq(3, 3, 1)))

      val result = SqlFilter(s"$FIELD_1.$FIELD_1.$FIELD_1 = 'row21'").transform(df)

      result.isRight shouldBe true

      result.map { df =>
        df.count shouldBe 1
      }
    }
}
