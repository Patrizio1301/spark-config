package Row

import transformation.transformations.{Regex, RegexColumn}
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
class RegexColumnTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  val inputSchema2 = NestedTwoLevels(2, 2)

  test("Create new column correctly") {

      val content = Seq(
        Row("aaa", ""),
        Row("bbb", ""),
        Row("aca", ""),
        Row("adb", ""),
        Row("aea", "")
      )

      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RegexColumn(
        regexPattern = "a(.?)a",
        columnToRegex = FIELD_1,
        regex = Seq(Regex(regexGroup = 1, field = FIELD_3))
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsSet(result, FIELD_3) shouldBe Set("a", "", "c", "e")
      }
    }

    test("Create two new columns correctly") {
      val content = Seq(
        Row(Row("aaa.bbb", ""), ""),
        Row(Row(".bbb", ""), ""),
        Row(Row("aba.", ""), ""),
        Row(Row("cdba", ""), "")
      )

      val df = createDataFrame(content, inputSchema2)

      val result = RegexColumn(
        regexPattern = "([a-z]*)\\.([a-z]*)",
        columnToRegex = "A.A",
        regex = Seq(
          Regex(regexGroup = 1, field = "regex.A"),
          Regex(regexGroup = 2, field = "regex.B")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        assert(result.count() == 4)
        getColumnAsSet(result, s"regex.$FIELD_1") shouldBe Set("aaa", "aba", "")
        getColumnAsSet(result, s"regex.$FIELD_2") shouldBe Set("bbb", "")
      }
    }

  test("with nested structure") {

      val content = Seq(
        Row(Row(Row("dd"), "123jjk", "AA"), "BB", "AA"),
        Row(Row(Row("dd"), "123jjk", "AA"), "BB", "AA"),
        Row(Row(Row("dd"), "jjk", "AA"), "BB", "AA")
      )

      val df: DataFrame =
        createDataFrame(content,
                        nestedSchema(Seq(StringType, StringType, StringType), Seq(3, 3, 1)))

      val result = RegexColumn(
        regexPattern = "([0-9]+)",
        columnToRegex = s"$FIELD_1.$FIELD_2",
        regex = Seq(
          Regex(regexGroup = 1, field = s"$FIELD_1.$FIELD_4")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.collect().toSet shouldBe Set(
          Row(Row(Row("dd"), "123jjk", "AA", "123"), "BB", "AA"),
          Row(Row(Row("dd"), "123jjk", "AA", "123"), "BB", "AA"),
          Row(Row(Row("dd"), "jjk", "AA", ""), "BB", "AA")
        )
      }
    }

    test("with nested structure nivel 2") {

      val content = Seq(
        Row(Row(Row("11aa"), "123jjk", "AA"), "BB", "AA"),
        Row(Row(Row("22dd"), "123jjk", "AA"), "BB", "AA"),
        Row(Row(Row("dd"), "jjk", "AA"), "BB", "AA")
      )

      val df: DataFrame =
        createDataFrame(content,
                        nestedSchema(Seq(StringType, StringType, StringType), Seq(3, 3, 1)))

      val result = RegexColumn(
        regexPattern = "([0-9]+)",
        columnToRegex = s"$FIELD_1.$FIELD_1.$FIELD_1",
        regex = Seq(
          Regex(regexGroup = 1, field = s"$FIELD_1.$FIELD_4")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.collect().toSet shouldBe Set(
          Row(Row(Row("11aa"), "123jjk", "AA", "11"), "BB", "AA"),
          Row(Row(Row("22dd"), "123jjk", "AA", "22"), "BB", "AA"),
          Row(Row(Row("dd"), "jjk", "AA", ""), "BB", "AA")
        )
      }
    }

}
