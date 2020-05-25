package Row

import transformation.transformations.CleanNulls
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
class CleanNullsTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("CleanNulls only one field") {
    val content = Seq(
      Row("aa"),
      Row("aa"),
      Row(null)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, StringType))

    val result = CleanNulls(
      primaryKey = Some(Seq(FIELD_1))
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.count() shouldBe 2
      result.collect().toSet shouldBe Set(Row("aa"))
    }
  }

  test("CleanNulls only one field with all null") {

    val content = Seq(
      Row(null),
      Row(null),
      Row(null)
    )

    val df: DataFrame = createDataFrame(content, inputSchema(1, StringType))

    val result = CleanNulls(
      primaryKey = Some(Seq(FIELD_1))
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.count() shouldBe 0
    }
  }

  test("CleanNulls more than one field and one value for primary keys") {

    val content = Seq(
      Row(null, "aa"),
      Row("bb", null),
      Row("cc", "cc")
    )

    val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

    val result = CleanNulls(
      primaryKey = Some(Seq(FIELD_1))
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.count() shouldBe 2
      result.collect().toSet shouldBe Set(Row("bb", null), Row("cc", "cc"))
    }
  }

  test("CleanNulls for all columns if never of them has primary keys") {

    val content = Seq(
      Row(null, "aa"),
      Row("bb", null),
      Row("cc", "cc")
    )
    val df: DataFrame = createDataFrame(content, inputSchema(2, StringType))

    val result = CleanNulls(
      ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.count() shouldBe 1
      result.collect().toSet shouldBe Set(Row("cc", "cc"))
    }
  }

  test("with nested structure") {

    val content = Seq(
      Row(Row(Row("dd"), null, 2), "BB", "AA"),
      Row(Row(Row("dd"), 2, 2), "BB", "AA")
    )
    val df: DataFrame =
      createDataFrame(content, nestedSchema(Seq(StringType, IntegerType, StringType), Seq(3, 3, 1)))

    val result = CleanNulls(
      primaryKey = Some(Seq(s"$FIELD_1.$FIELD_2"))
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.collect.toSet shouldBe Set(Row(Row(Row("dd"), 2, 2), "BB", "AA"))
    }
  }

  test("with nested structure level 2") {

    val content = Seq(
      Row(Row(Row("dd"), null, 2), "BB", "AA"),
      Row(Row(Row("dd"), 2, 2), "BB", "AA")
    )
    val df: DataFrame =
      createDataFrame(content, nestedSchema(Seq(StringType, IntegerType, StringType), Seq(3, 3, 1)))

    val result = CleanNulls(
      primaryKey = Some(Seq(s"$FIELD_1.$FIELD_1.$FIELD_1"))
    ).transform(df)

    result.isRight shouldBe true

    result.map { result =>
      result.collect.toSet shouldBe Set(
        Row(Row(Row("dd"), null, 2), "BB", "AA"),
        Row(Row(Row("dd"), 2, 2), "BB", "AA")
      )
    }
  }
}
