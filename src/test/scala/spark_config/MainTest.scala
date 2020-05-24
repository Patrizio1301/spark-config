package spark_config

import utils.test.InitSparkSessionFunSuite
import cats.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import transformation.Transform._
import transformation.Transformations._
import transformation.transformations.Base64
import utils.test.InitSparkSessionFunSuite


@RunWith(classOf[JUnitRunner])
class MainTest
  extends InitSparkSessionFunSuite
    with GivenWhenThen
    with Matchers {

  import spark.implicits._

  test("first configuration file") {
    Main.main("/home/patrizio/Documentos/spark-config/src/test/resources/example.conf")
  }

  test("write in base64") {

    val df = List("spark", "gerson").toDF("test")

    val dfResult = Base64(
      field = "test"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, "test") shouldBe Set("c3Bhcms=", "Z2Vyc29u")
    }
  }

  test("unwrite in base64") {
    val df = List("c3Bhcms=", "Z2Vyc29u").toDF("test")

    val dfResult = Base64(
      field = "test",
      encrypted = Some(true)
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, "test") shouldBe Set("spark", "gerson")
    }
  }

  test("write in base64 with integerType") {

    val df = List(1, 2).toDF("test")

    val dfResult = Base64(
      field = "test"
    ).transform(df)

    dfResult.isLeft shouldBe true

    Then("text should be in uppercase")
    dfResult.leftMap { error =>
      error.toString() shouldBe
        "Error in transformation Base64: The column test cannot be casted to base64, " +
          "since the data type is IntegerType and not Stringtype."
    }
  }
}
