package Row

import com.typesafe.config.ConfigFactory
import transformation.transformations.RenameColumns
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
class RenameColumnsTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("rename all columns correctly") {

      val content = Seq(
        Row("aa", ""),
        Row("bb", ""),
        Row("cc", "")
      )

      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RenameColumns(
        columnsToRename = Map("A" -> "A_renamed", "B" -> "B_renamed2")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.count() shouldBe 3
        result.columns.length shouldBe 2
        assert(result.schema.fields.exists(_.name == "A_renamed"))
        assert(result.schema.fields.exists(_.name == "B_renamed2"))
      }
    }

    test("rename some columns correctly") {

      val content = Seq(
        Row("aa", ""),
        Row("bb", ""),
        Row("cc", "")
      )

      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RenameColumns(
        columnsToRename = Map("A" -> "A_renamed")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.count() shouldBe 3
        result.columns.length shouldBe 2
        assert(result.schema.fields.exists(_.name == "A_renamed"))
        assert(result.schema.fields.exists(_.name == FIELD_2))
      }
    }

    test("dont rename columns") {

      val config = ConfigFactory.parseString("""
          |{
          |  type : "renamecolumns"
          |  columnsToRename : {
          |  }
          |}
        """.stripMargin)

      val content = Seq(
        Row("aa", ""),
        Row("bb", ""),
        Row("cc", "")
      )

      val df = createDataFrame(content, inputSchema(2, StringType))

      val result = RenameColumns(
        columnsToRename = Map()
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.count() shouldBe 3
        result.columns.length shouldBe 2

        assert(result.schema.fields.exists(_.name == FIELD_1))
        assert(result.schema.fields.exists(_.name == FIELD_2))

      }
    }

    test("rename column with parenthesis correctly") {

      import spark.implicits._
      val df = List(("aa", 1), ("bb", 34), ("cc", 12)).toDF("col1", "sum(column)")

      val result = RenameColumns(
        columnsToRename = Map("sum(column)" -> "sum_column")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.count() shouldBe 3
        result.columns.length shouldBe 2
        assert(result.schema.fields.exists(_.name == "col1"))
        assert(result.schema.fields.exists(_.name == "sum_column"))
      }

    }

  test("rename in nested structure") {

      val content = Seq(Row(Row(Row("DDD"), "cc", "AAA"), "BB", "AA"))
      val df = createDataFrame(content,
                               nestedSchema(Seq(StringType, StringType, StringType), Seq(3, 3, 1)))

      val result = RenameColumns(
        columnsToRename = Map(FIELD_3 -> s"$FIELD_1.$FIELD_1.$FIELD_2")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.schema shouldBe nestedSchema(Seq(StringType, StringType, StringType), Seq(2, 3, 2))
      }
    }

    test("rename in nested structure 2") {

      val content = Seq(Row(Row(Row("DDD"), "cc", "AAA"), "BB", "AA"))
      val df = createDataFrame(content,
                               nestedSchema(Seq(StringType, StringType, StringType), Seq(3, 3, 1)))

      val result = RenameColumns(
        columnsToRename = Map(s"$FIELD_1.$FIELD_1.$FIELD_1" -> s"$FIELD_1.$FIELD_1")
      ).transform(df)

      result.isRight shouldBe true

      result.map { result =>
        result.schema shouldBe NestedTwoLevels(3, 3)
      }
    }
}
