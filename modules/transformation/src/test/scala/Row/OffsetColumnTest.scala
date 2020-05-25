package Row

import transformation.transformations.OffsetColumn
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
class OffsetColumnTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  private val targetColumn = "lagged_field"

  private val content = Seq(
    Row("20180901", 40.01),
    Row("20180902", 121.79),
    Row("20180903", 34.53)
  )

    test("Lag a column with default offset and default target column name") {

      val df = createDataFrame(content, simpleSchema(Seq(StringType, DoubleType)))

      val result = OffsetColumn(
        offsetType = "lag",
        columnToLag = FIELD_2,
        columnToOrderBy = FIELD_1
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsSet(result, s"${FIELD_2}_lag1") shouldBe Set(null, 40.01, 121.79)
      }
    }

    test("Lag a column with default offset and target column name") {

      val df = createDataFrame(content, simpleSchema(Seq(StringType, DoubleType)))

      val result = OffsetColumn(
        offsetType = "lag",
        columnToLag = FIELD_2,
        columnToOrderBy = FIELD_1,
        newColumnName = Some(targetColumn)
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsSet(result, targetColumn) shouldBe Set(null, 40.01, 121.79)
      }
    }

    test("Lead a column with default offset and target column name") {

      val df = createDataFrame(content, simpleSchema(Seq(StringType, DoubleType)))
      val result = OffsetColumn(
        offsetType = "lead",
        columnToLag = FIELD_2,
        columnToOrderBy = FIELD_1,
        newColumnName = Some(targetColumn)
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsSet(result, targetColumn) shouldBe Set(121.79, 34.53, null)
      }
    }

    test("Lead a column with custom offset and target column name") {

      val df = createDataFrame(content, simpleSchema(Seq(StringType, DoubleType)))

      val result = OffsetColumn(
        offsetType = "lead",
        columnToLag = FIELD_2,
        columnToOrderBy = FIELD_1,
        offset = Some(2),
        newColumnName = Some(targetColumn)
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        getColumnAsSet(result, targetColumn) shouldBe Set(34.53, null)
      }
    }

    test("with nested structure") {
      val content = Seq(
        Row(Row(Row("dd"), 1, 2), "BB", "AA"),
        Row(Row(Row("dd"), 1, 2), "BB", "AA"),
        Row(Row(Row("dd"), 1, 3), "BB", "AA")
      )
      val df: DataFrame =
        createDataFrame(content,
                        nestedSchema(Seq(StringType, IntegerType, StringType), Seq(3, 3, 1)))

      val result = OffsetColumn(
        offsetType = "lag",
        columnToLag = s"$FIELD_1.$FIELD_2",
        columnToOrderBy = s"$FIELD_1.$FIELD_3",
        newColumnName = Some(s"$FIELD_1.lagged_field")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.collect().toSet shouldBe Set(
          Row(Row(Row("dd"), 1, 2, null), "BB", "AA"),
          Row(Row(Row("dd"), 1, 2, 1), "BB", "AA"),
          Row(Row(Row("dd"), 1, 3, 1), "BB", "AA")
        )
      }
    }

    test("with nested structure level two") {

      val content = Seq(
        Row(Row(Row("aa", "bb"), 1, 2), "BB", "AA"),
        Row(Row(Row("dd", "ee"), 1, 2), "BB", "AA"),
        Row(Row(Row("dd", "HH"), 1, 3), "BB", "AA")
      )
      val df = createDataFrame(content,
                               nestedSchema(Seq(StringType, IntegerType, StringType), Seq(3, 3, 2)))

      val result = OffsetColumn(
        offsetType = "lag",
        columnToLag = s"$FIELD_1.$FIELD_1.$FIELD_1",
        columnToOrderBy = s"$FIELD_1.$FIELD_1.$FIELD_2",
        newColumnName = Some(s"$FIELD_1.$FIELD_1.lagged_field")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.collect().toSet shouldBe Set(
          Row(Row(Row("dd", "HH", null), 1, 3), "BB", "AA"),
          Row(Row(Row("aa", "bb", "dd"), 1, 2), "BB", "AA"),
          Row(Row(Row("dd", "ee", "aa"), 1, 2), "BB", "AA")
        )
      }
    }

    test("with nested structure level one plus two") {

      val content = Seq(
        Row(Row(Row("aa", "bb"), 1, 2), "BB", "AA"),
        Row(Row(Row("dd", "ee"), 1, 2), "BB", "AA"),
        Row(Row(Row("dd", "HH"), 1, 3), "BB", "AA")
      )
      val df = createDataFrame(content,
                               nestedSchema(Seq(StringType, IntegerType, StringType), Seq(3, 3, 2)))

      val result = OffsetColumn(
        offsetType = "lead",
        columnToLag = s"$FIELD_1.$FIELD_3",
        columnToOrderBy = s"$FIELD_1.$FIELD_1.$FIELD_2",
        newColumnName = Some(s"$FIELD_1.$FIELD_1.lagged_field")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.collect().toSet shouldBe Set(
          Row(Row(Row("dd", "HH", 2), 1, 3), "BB", "AA"),
          Row(Row(Row("aa", "bb", 2), 1, 2), "BB", "AA"),
          Row(Row(Row("dd", "ee", null), 1, 2), "BB", "AA")
        )
      }
    }
}
