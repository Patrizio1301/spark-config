package Row

import transformation.transformations.ConcatColumns
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
class ConcatColumnsTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

    val fieldResult = "result"

    test("Concatenate a column properly without separator") {

      val content = Seq(
        Row("aa1", "bb1"),
        Row("aa2", "bb2"),
        Row("aa3", "bb3")
      )

      val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

      val result = ConcatColumns(
        columnsToConcat = Seq(FIELD_1, FIELD_2),
        columnName = fieldResult
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        assert(result.count() == 3)
        assert(result.columns.length == 3)
        result.select(s"$FIELD_1", s"$FIELD_2").collect shouldBe df.collect()
        getColumnAsSet(result, fieldResult) shouldBe Set("aa1bb1", "aa2bb2", "aa3bb3")
      }
    }

    test("Concatenate a column properly with separator") {

      val content = Seq(
        Row("aa1", "bb1"),
        Row("aa2", "bb2"),
        Row("aa3", "bb3")
      )

      val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

      val result = ConcatColumns(
        columnsToConcat = Seq(FIELD_1, FIELD_2),
        columnName = fieldResult,
        separator = Some("###")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        assert(result.count() == 3)
        assert(result.columns.length == 3)
        result.select(s"$FIELD_1", s"$FIELD_2").collect shouldBe df.collect()
        getColumnAsSet(result, fieldResult) shouldBe Set("aa1###bb1", "aa2###bb2", "aa3###bb3")
      }
    }

    test("Concatenate just one column with separator") {

      val content = Seq(
        Row("aa1"),
        Row("aa2"),
        Row("aa3")
      )

      val df: DataFrame = createDataFrame(content, inputSchema(1, StringType))

      val result = ConcatColumns(
        columnsToConcat = Seq(FIELD_1),
        columnName = fieldResult,
        separator = Some("###")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        assert(result.count() == 3)
        assert(result.columns.length == 2)
        result.select(s"$FIELD_1").collect shouldBe df.collect()
        getColumnAsSet(result, fieldResult) shouldBe Set("aa1", "aa2", "aa3")
      }

    }

    test("Concatenate two columns without a separator, converting nulls 'hello'") {

      val content = Seq(
        Row("Why", null),
        Row(null, "There"),
        Row("General", "Kenobi")
      )

      val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

      val result = ConcatColumns(
        columnsToConcat = Seq(FIELD_1, FIELD_2),
        columnName = fieldResult,
        convertNulls = Some("Hello")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        assert(result.count() == 3)
        assert(result.columns.length == 3)
        getColumnAsSet(result, fieldResult) shouldBe Set("WhyHello", "HelloThere", "GeneralKenobi")
      }

    }

    test("Concatenate two columns with separator, converting nulls to empty string") {

      Given("A configuration")

      val content = Seq(
        Row("aa1", null),
        Row(null, "bb2"),
        Row("aa3", "bb3")
      )

      val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

      val result = ConcatColumns(
        columnsToConcat = Seq(FIELD_1, FIELD_2),
        columnName = fieldResult,
        separator = Some("|"),
        convertNulls = Some("")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        assert(result.count() == 3)
        assert(result.columns.length == 3)
        getColumnAsSet(result, fieldResult) shouldBe Set("aa1|", "|bb2", "aa3|bb3")
      }

    }

    test("Concatenate three columns with separator, converting nulls to 'null'") {

      val content = Seq(
        Row(null, null, "cc1"),
        Row("aa2", null, null),
        Row(null, "bb3", null),
        Row(null, null, null)
      )

      val df: DataFrame = createDataFrame(content, inputSchema(3, StringType))

      val result = ConcatColumns(
        columnsToConcat = Seq(FIELD_1, FIELD_2, FIELD_3),
        columnName = fieldResult,
        separator = Some("|"),
        convertNulls = Some("null")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        assert(result.count() == 4)
        assert(result.columns.length == 4)
        getColumnAsSet(result, fieldResult) shouldBe Set(
          "null|null|cc1",
          "aa2|null|null",
          "null|bb3|null",
          "null|null|null"
        )
      }
    }

    test("Concatenate a non-existent column") {

      val content = Seq(
        Row("aa1", "bb1"),
        Row("aa2", "bb2"),
        Row("aa3", "bb3")
      )

      val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

      val result = ConcatColumns(
        columnsToConcat = Seq(FIELD_1, "columnUnknown"),
        columnName = fieldResult,
        separator = Some("###")
      ).transform(df)

      result.isLeft shouldBe true

      result.leftMap(error =>
        error.toString shouldBe
        "Error in transformation ConcatColumns: The field columnUnknown is not contained in the current dataFrame.")

    }

    test("Concatenate no columns") {

      val content = Seq(
        Row("aa1", "bb1"),
        Row("aa2", "bb2"),
        Row("aa3", "bb3")
      )

      val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

      val result = ConcatColumns(
        columnsToConcat = Seq(),
        columnName = fieldResult,
        separator = Some("###")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        assert(result.count() == 3)
        assert(result.columns.length == 3)
        result.select(s"$FIELD_1", s"$FIELD_2").collect shouldBe df.collect()
        getColumnAsSet(result, fieldResult) shouldBe Set("")
      }
    }

    test("with nested structure") {
      val content = Seq(Row(Row(Row("dd"), "12", "12"), "BB", "AA"))
      val df: DataFrame =
        createDataFrame(content,
                        nestedSchema(Seq(StringType, StringType, StringType), Seq(3, 3, 1)))

      val result = ConcatColumns(
        columnsToConcat = Seq(s"$FIELD_1.$FIELD_2", s"$FIELD_1.$FIELD_3"),
        columnName = s"$FIELD_1.$FIELD_4"
      ).transform(df)

      result.isRight shouldBe true

      result.map { df =>
        getColumnAsSet(df, s"$FIELD_1.$FIELD_4") shouldBe Set("1212")
      }
    }

    test("with nested structure nivel 2") {
      val content = Seq(Row(Row(Row("DDD", "AAA"), "cc", "AAA"), "BB", "AA"))
      val df: DataFrame =
        createDataFrame(content,
                        nestedSchema(Seq(StringType, StringType, StringType), Seq(3, 3, 1)))

      val result = ConcatColumns(
        columnsToConcat = Seq(s"$FIELD_1.$FIELD_1.$FIELD_1", s"$FIELD_1.$FIELD_2"),
        columnName = s"$FIELD_1.$FIELD_1.$FIELD_3"
      ).transform(df)

      result.isRight shouldBe true

      result.map { df =>
        getColumnAsSet(df, s"$FIELD_1.$FIELD_1.$FIELD_3") shouldBe Set("DDDcc")
      }
    }
}
