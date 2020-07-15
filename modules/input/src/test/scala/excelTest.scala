import utils.test.InitSparkSessionFunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import cats.implicits._
import input.inputs._
import input.Inputs._

@RunWith(classOf[JUnitRunner])
class excelTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  import spark.implicits._
  test("read excel file") {

    val path = "/home/patrizio-guagliardo/IdeaProjects/spark-config/modules/input/src/test/resources/excel_test.xlsx"

    val input = Excel(path = path, sheetName = "input")

    InputUtils.inputGeneric(spark)(input)
      .map(
        df => df.show()
      )
  }
}
