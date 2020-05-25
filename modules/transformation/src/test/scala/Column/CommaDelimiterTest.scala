package Column

import com.typesafe.config.ConfigFactory
import transformation.transformations.CommaDelimiter
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import transformation.Transform._
import transformation.Transformations._

@RunWith(classOf[JUnitRunner])
class CommaDelimiterTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("Convert a column to decimal number") {
    Given("config")

    val config = ConfigFactory.parseString(s"""
           |      {
           |        field = "$FIELD_1"
           |        type = "commaDelimiter"
           |        lengthDecimal = 3
           |        separatorDecimal= "."
           |      }
        """.stripMargin)

    Given("column to parse")
    val content = Seq(
      Row("18243"),
      Row("8"),
      Row("127"),
      Row("2212")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    When("apply transformation.transformations")
    val result = CommaDelimiter(
      field = FIELD_1,
      lengthDecimal = Some(3),
      separatorDecimal = Some(".")
    ).transform(df)

    result.isRight shouldBe true

    result.map { df =>
      getColumnAsSet(df, FIELD_1) shouldBe Set(
        "18.243",
        "0.008",
        "0.127",
        "2.212"
      )
    }
  }
}
