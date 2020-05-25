package Column

import transformation.transformations.CopyColumn
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
class CopyColumnTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("copy column integer") {

    val content = Seq(
      Row(0),
      Row(1),
      Row(2)
    )

    val df = createDataFrame(content, inputSchema(1, IntegerType))

    val dfResult = CopyColumn(
      field = "columnWithValueCopied",
      copyField = FIELD_1
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      val resultList: Array[Row] = result.select("columnWithValueCopied").collect

      resultList.contains(Row(0)) shouldBe true
      resultList.contains(Row(1)) shouldBe true
      resultList.contains(Row(2)) shouldBe true
      resultList.contains(Row(3)) shouldBe false

      resultList.length shouldBe 3

      assert(
        result.schema.fields.tail.forall(
          _.dataType === IntegerType
        )
      )
    }
  }

  test("copy column with cast") {

    val content = Seq(
      Row(0),
      Row(1),
      Row(2)
    )

    val df = createDataFrame(content, inputSchema(1, IntegerType))

    val dfResult = CopyColumn(
      field = "columnWithValueCopied",
      copyField = FIELD_1,
      defaultType = Some("double")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      val resultList: Array[Row] = result.select("columnWithValueCopied").collect

      assert(
        result.schema.fields.tail.forall(
          _.dataType === DoubleType
        )
      )

      resultList.contains(Row(0D)) shouldBe true
      resultList.contains(Row(1D)) shouldBe true
      resultList.contains(Row(2D)) shouldBe true
      resultList.contains(Row(3D)) shouldBe false

      resultList.length shouldBe 3
    }
  }
}
