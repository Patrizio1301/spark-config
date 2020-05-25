package Column

import cats.implicits._
import com.typesafe.config.ConfigFactory
import transformation.transformations.Trimmer
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
class TrimmerTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("trim strings by different sides") {

    val testData = "    strTest      "
    val content  = Seq(Row(testData, testData, testData, testData))
    val df       = createDataFrame(content, inputSchema(4, StringType))

    val dfResult = Trimmer(
      field = FIELD_1,
      trimType = Some("both")
    ).transform(df).flatMap { df2 =>
      Trimmer(
        field = FIELD_2,
        trimType = Some("right")
      ).transform(df2).flatMap { df3 =>
        Trimmer(
          field = FIELD_3,
          trimType = Some("left")
        ).transform(df3).flatMap { df4 =>
          Trimmer(
            field = FIELD_4,
            trimType = Some("unknown")
          ).validate.map(op => op.transform(df4))
        }
      }
    }

    dfResult.isLeft shouldBe true
    dfResult.leftMap { result =>
      result.toString() shouldBe "Error in transformation Trimmer: The paramater trimType has the invalid value unknown. "
    }
  }

  test("trim values with different data types") {
    Given("config")

    val config = ConfigFactory.parseString(s"""
           |    transformation.transformations = [
           |      {
           |        field = "$FIELD_1"
           |        type = "trim"
           |      },
           |      {
           |        field = "$FIELD_2"
           |        type = "trim"
           |      },
           |      {
           |        field = "$FIELD_3"
           |        type = "trim"
           |      }
           |    ]
        """.stripMargin)

    Given("column to parse")

    val content = Seq(
      Row(10, false, None.orNull)
    )

    val df = createDataFrame(content, simpleSchema(Seq(IntegerType, BooleanType, NullType)))

    val dfResult = Trimmer(
      field = FIELD_1
    ).transform(df).flatMap { df2 =>
      Trimmer(
        field = FIELD_2
      ).transform(df2).flatMap { df3 =>
        Trimmer(
          field = FIELD_3
        ).transform(df3)
      }
    }

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      val resultList: Array[Row] = result.select(s"$FIELD_1", s"$FIELD_2", s"$FIELD_3").collect
      resultList(0).get(0) shouldBe "10"
      resultList(0).get(1) shouldBe "false"
      resultList(0).get(2) should be eq None.orNull
      resultList.length shouldBe 1
      assert(result.schema.fields.tail.forall(_.dataType === StringType))
    }

  }

  private def trimLeft(str: String) = str.dropWhile(_.isWhitespace)

  private def trimRight(str: String) = trimLeft(str.reverse).reverse

}
