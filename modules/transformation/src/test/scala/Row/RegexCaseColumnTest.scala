package Row

import transformation.transformations.{RegexCaseColumn, RegexConfig}
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import transformation.Transform._
import transformation.Transformations._

@RunWith(classOf[JUnitRunner])
class RegexCaseColumnTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("Create new column with one regex") {
      val content = Seq(
        Row("aaa", "aaa"),
        Row("aabb", "abaaa"),
        Row("acaaaaa", "arrr"),
        Row("adb", "atat"),
        Row("aaaeaaa", "taaataaa")
      )
      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RegexCaseColumn(
        field = "column3",
        default = Some("NotFound"),
        regexList = Seq(
          RegexConfig(pattern = ".*aaa.*", columnToRegex = FIELD_1, value = "FoundIt")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map(
        result =>
          getColumnAsList(result, "column3") shouldBe List(
            "FoundIt",
            "NotFound",
            "FoundIt",
            "NotFound",
            "FoundIt"
        )
      )
    }

    test("Create new column with two regexes on one column") {

      val content = Seq(
        Row("aaa", "aaa"),
        Row("999", "abaaa"),
        Row("1258", "arrr"),
        Row("adb!@%", "atat"),
        Row("aaa%eaaa", "taaataaa")
      )

      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RegexCaseColumn(
        field = "column3",
        default = Some("String"),
        regexList = Seq(
          RegexConfig(pattern = "[!|@|#|%|^|&|*|(|)|~]+",
                      columnToRegex = FIELD_1,
                      value = "Special Chars"),
          RegexConfig(pattern = "[0-9]+", columnToRegex = FIELD_1, value = "Numerical")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsList(result, "column3") shouldBe List(
          "String",
          "Numerical",
          "Numerical",
          "Special Chars",
          "Special Chars"
        )
      }
    }

    test("Create new column with two regexes on one column, with priority mattering") {
      val content = Seq(
        Row("aaa", "aaa!"),
        Row("9!9!9", "abaaa!"),
        Row("1258", "arrr"),
        Row("adb!@%", "atat"),
        Row("aaa%eaaa", "taaataaa")
      )

      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RegexCaseColumn(
        field = "column3",
        default = Some("String"),
        regexList = Seq(
          RegexConfig(pattern = "[!|@|#|%|^|&|*|(|)|~]+",
                      columnToRegex = FIELD_1,
                      value = "Special Chars"),
          RegexConfig(pattern = "[0-9]+", columnToRegex = FIELD_1, value = "Numerical")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsList(result, "column3") shouldBe List(
          "String",
          "Special Chars",
          "Numerical",
          "Special Chars",
          "Special Chars"
        )
      }
    }

    test("Create new column with three regexes on two different columns") {

      val content = Seq(
        Row("aaa", "aaa!"),
        Row("999", "abaaa!"),
        Row("1258", "arrr"),
        Row("adb!@%", "atat"),
        Row("aaaeaaa", "taaataaa")
      )

      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RegexCaseColumn(
        field = "column3",
        default = Some("String"),
        regexList = Seq(
          RegexConfig(pattern = "[!|@|#|%|^|&|*|(|)|~]+",
                      columnToRegex = FIELD_1,
                      value = "Special Chars"),
          RegexConfig(pattern = "[!|@|#|%|^|&|*|(|)|~]+",
                      columnToRegex = FIELD_2,
                      value = "Special Chars"),
          RegexConfig(pattern = "[0-9]+", columnToRegex = FIELD_1, value = "Numerical")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsList(result, "column3") shouldBe List(
          "Special Chars",
          "Special Chars",
          "Numerical",
          "Special Chars",
          "String"
        )
      }
    }

    test("Create new column with three regexes on three different columns") {
      val content = Seq(
        Row("aaa", "aaa!", "aaa"),
        Row("999", "abaaa!", "1234"),
        Row("1258", "arrr", "hello"),
        Row("adb!@%", "atat", "!#$%"),
        Row("aaaeaaa", "3333", "atatat")
      )

      val df = createDataFrame(content, inputSchema(3, StringType))

      val result = RegexCaseColumn(
        field = "column4",
        default = Some("Unmatched"),
        regexList = Seq(
          RegexConfig(pattern = "[!|@|#|%|^|&|*|(|)|~]+",
                      columnToRegex = FIELD_1,
                      value = "Special Chars"),
          RegexConfig(pattern = "[0-9]+", columnToRegex = FIELD_2, value = "Numerical"),
          RegexConfig(pattern = "[a-z|A-Z]+", columnToRegex = FIELD_3, value = "Alphabetical")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsList(result, "column4") shouldBe List(
          "Alphabetical",
          "Unmatched",
          "Alphabetical",
          "Special Chars",
          "Numerical"
        )
      }
    }

    test("Override an existing column with a regex, defaulting to the column's current values") {

      val content = Seq(
        Row("aaa", "aaa!"),
        Row("Hello World", "abaaa!"),
        Row("1258", "arrr"),
        Row("Hello", "atat"),
        Row("World", "3333")
      )

      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RegexCaseColumn(
        field = FIELD_2,
        default = Some("This should not Matter"),
        regexList = Seq(
          RegexConfig(pattern = "[Hello|World]+", columnToRegex = FIELD_1, value = "Greetings!")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsList(result, FIELD_2) shouldBe List(
          "aaa!",
          "Greetings!",
          "arrr",
          "Greetings!",
          "Greetings!"
        )
      }
    }

    test("Perform the regex without matching anything") {

      val content = Seq(
        Row("aaa", "aaa!"),
        Row("Hello World", "abaaa!"),
        Row("1258", "arrr"),
        Row("Hello", "atat"),
        Row("World", "3333")
      )
      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RegexCaseColumn(
        field = "column3",
        default = Some("Not Here"),
        regexList = Seq(
          RegexConfig(pattern = "Waldo", columnToRegex = FIELD_1, value = "Found Him!")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsList(result, "column3") shouldBe List(
          "Not Here",
          "Not Here",
          "Not Here",
          "Not Here",
          "Not Here"
        )
      }
    }

    test("Perform the regex without matching anything on an existing column") {

      val content = Seq(
        Row("aaa", "aaa!"),
        Row("Hello World", "abaaa!"),
        Row("1258", "arrr"),
        Row("Hello", "atat"),
        Row("World", "3333")
      )
      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RegexCaseColumn(
        field = FIELD_2,
        default = Some("Not Here"),
        regexList = Seq(
          RegexConfig(pattern = "Waldo", columnToRegex = FIELD_1, value = "Found Him!")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsList(result, FIELD_2) shouldBe List(
          "aaa!",
          "abaaa!",
          "arrr",
          "atat",
          "3333"
        )
      }
    }

    test("Create the transformation without a default value") {

      val content = Seq(
        Row("aaa", "aaa!"),
        Row("Hello World", "abaaa!"),
        Row("1258", "arrr"),
        Row("aba", "atat"),
        Row("aaabaaa", "3333")
      )
      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RegexCaseColumn(
        field = FIELD_3,
        regexList = Seq(
          RegexConfig(pattern = ".*aaa.*", columnToRegex = FIELD_1, value = "FoundIt")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsList(result, FIELD_3) shouldBe List(
          "FoundIt",
          "",
          "",
          "",
          "FoundIt"
        )
      }
    }

    test("Create the transformation without a single regex") {

      val content = Seq(
        Row("aaa", "aaa!"),
        Row("Hello World", "abaaa!"),
        Row("1258", "arrr"),
        Row("Hello", "atat"),
        Row("World", "3333")
      )
      val df = createDataFrame(content, inputSchema(2, StringType))

//      val result=Regexcasecolumn.validated(
//          field = FIELD_2,
//          default = Some("This should not Matter"),
//          regexList = Seq()
//        )
//
//      result.isLeft shouldBe true
//
//      result.leftMap{error =>
//          error.toString shouldBe "Error in transformation RegexCaseColumn: " +
//            "The parameter These operations cannot be performed on one or zero columns. " +
//            "Add two or more column names into valuesToOperateOn: [<column1>, <column2>] is missing."
//        }
    }

    test("Attempt to regex a non-existent column") {

      val content = Seq(
        Row("aaa", "aaa!"),
        Row("Hello World", "abaaa!"),
        Row("1258", "arrr"),
        Row("Hello", "atat"),
        Row("World", "3333")
      )
      val df = createDataFrame(content, inputSchema(2, StringType))

      When("Perform the regex")
      assertThrows[AnalysisException] {
        RegexCaseColumn(
          field = "column4",
          regexList = Seq(
            RegexConfig(pattern = "ERROR", columnToRegex = FIELD_3, value = "Column Doesn't Exist")
          )
        ).transform(df)
      }
    }

  test("with nested structure") {

      val content = Seq(
        Row(Row(Row("dd"), 1, 2), "BB", "AA"),
        Row(Row(Row("dd"), 5, 2), "BB", "AA"),
        Row(Row(Row("dd"), 7, 3), "BB", "AA")
      )
      val df = createDataFrame(content,
                               nestedSchema(Seq(StringType, IntegerType, StringType), Seq(3, 3, 1)))

      val result = RegexCaseColumn(
        field = s"$FIELD_1.$FIELD_4",
        default = Some("NotFound"),
        regexList = Seq(
          RegexConfig(pattern = ".*1.*", columnToRegex = s"$FIELD_1.$FIELD_2", value = "FoundIt"),
          RegexConfig(pattern = ".*dd.*", columnToRegex = s"$FIELD_1.$FIELD_3", value = "FoundIt")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.collect().toSet shouldBe Set(
          Row(Row(Row("dd"), 1, 2, "FoundIt"), "BB", "AA"),
          Row(Row(Row("dd"), 5, 2, "NotFound"), "BB", "AA"),
          Row(Row(Row("dd"), 7, 3, "NotFound"), "BB", "AA")
        )
      }
    }

    test("with nested structure nivel 2") {

      val content = Seq(
        Row(Row(Row("aa"), 1, 2), "BB", "AA"),
        Row(Row(Row("dd"), 1, 2), "BB", "AA"),
        Row(Row(Row("dd"), 1, 3), "BB", "AA")
      )
      val df = createDataFrame(content,
                               nestedSchema(Seq(StringType, IntegerType, StringType), Seq(3, 3, 1)))

      val result = RegexCaseColumn(
        field = s"$FIELD_1.$FIELD_4",
        default = Some("NotFound"),
        regexList = Seq(
          RegexConfig(pattern = ".*dd.*",
                      columnToRegex = s"$FIELD_1.$FIELD_1.$FIELD_1",
                      value = "FoundIt")
        )
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.collect().toSet shouldBe Set(
          Row(Row(Row("aa"), 1, 2, "NotFound"), "BB", "AA"),
          Row(Row(Row("dd"), 1, 2, "FoundIt"), "BB", "AA"),
          Row(Row(Row("dd"), 1, 3, "FoundIt"), "BB", "AA")
        )
      }
    }
}
