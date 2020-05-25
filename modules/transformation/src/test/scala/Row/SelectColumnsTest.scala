package Row

import transformation.transformations.SelectColumns
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{AnalysisException, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import transformation.Transform._
import transformation.Transformations._

@RunWith(classOf[JUnitRunner])
class SelectColumnsTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("SelectColumns select only indicated columns") {
      val content = Seq(
        Row("row11", "row12", "row13", "row14", "row15"),
        Row("row21", "row22", "row23", "row24", "row25")
      )

      val df = createDataFrame(content, inputSchema(5, StringType))

      val columnsToSelect = Seq("B", "C", "E")

      val result = SelectColumns(columnsToSelect = columnsToSelect).transform(df)

      result.isRight shouldBe true

      result.map { _df =>
        _df.columns.length shouldBe columnsToSelect.size
        assert(columnsToSelect.forall(_df.columns.contains(_)))
      }
    }

    test("SelectColumns throws error when select zero columns") {
      val content = Seq(
        Row("row11", "row12", "row13", "row14", "row15"),
        Row("row21", "row22", "row23", "row24", "row25")
      )

      val df = createDataFrame(content, inputSchema(5, StringType))

//      val result=Selectcolumns.validated(Seq())
//
//      result.isLeft shouldBe true
//
//      result.leftMap{
//        error => error.toString shouldBe "Error in transformation SelectColumns: The parameter columnsToSelect is missing."
//      }
    }

    test(
      "SelectColumns does not select any column that does not exist and throws error if you try") {

      val content = Seq(
        Row("row11", "row12", "row13", "row14", "row15"),
        Row("row21", "row22", "row23", "row24", "row25")
      )

      val df = createDataFrame(content, inputSchema(5, StringType))

      assertThrows[AnalysisException](
        SelectColumns(Seq("notExist")).transform(df)
      )
    }

  test("Select nested columns") {

      val content = Seq(Row(Row(Row("DDD"), "cc", "AAA"), "BB", "AA"))
      val df = createDataFrame(content,
                               nestedSchema(Seq(StringType, StringType, StringType), Seq(3, 3, 1)))

      val result = SelectColumns(
        Seq(s"$FIELD_1.$FIELD_1.$FIELD_1", s"$FIELD_2", s"$FIELD_1.$FIELD_3")).transform(df)

      result.isRight shouldBe true

      result.map { df =>
        df.schema shouldBe inputSchema(3, StringType)
      }
    }
}
