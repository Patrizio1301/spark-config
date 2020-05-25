package Column

import transformation.transformations.{Formatter, Replacement}
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
class FormatterTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("cast to String") {

    val content = Seq(
      Row(1),
      Row(1),
      Row(1)
    )

    val df = createDataFrame(content, inputSchema(1, IntegerType))

    val dfResult = Formatter(
      field = FIELD_1,
      typeToCast = "string"
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      assert(result.schema.find(_.name == FIELD_1).get.dataType === StringType)
      getColumnAsSet(result, FIELD_1) shouldBe Set("1")
    }
  }

  test("cast to Integer") {

    val content = Seq(
      Row("1"),
      Row("1"),
      Row("1")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Formatter(
      field = FIELD_1,
      typeToCast = "int"
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      assert(result.schema.find(_.name == FIELD_1).get.dataType === IntegerType)
      getColumnAsSet(result, FIELD_1) shouldBe Set(1)
    }
  }

  test("cast to Decimal") {

    val content = Seq(
      Row("2.1"),
      Row("2.1"),
      Row("2.1")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Formatter(
      field = FIELD_1,
      typeToCast = "decimal(2,1)"
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      assert(result.schema.find(_.name == FIELD_1).get.dataType === DecimalType(2, 1))
      getColumnAsSet(result, FIELD_1) shouldBe Set(new java.math.BigDecimal("2.1"))
    }
  }

  test("cast to Date") {

    val content = Seq(
      Row("2017-07-16"),
      Row("2017-07-16"),
      Row("2017-07-16")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Formatter(
      field = FIELD_1,
      typeToCast = "date"
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      assert(result.schema.find(_.name == FIELD_1).get.dataType === DateType)
      assert(
        result
          .select(FIELD_1)
          .collect()
          .forall(
            _.getAs[java.util.Date](FIELD_1) != None.orNull
          ))
    }
  }

  test("cast to Date malformed") {

    val content = Seq(
      Row("malformed"),
      Row("malformed"),
      Row("malformed")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Formatter(
      field = FIELD_1,
      typeToCast = "date"
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      assert(result.schema.find(_.name == FIELD_1).get.dataType === DateType)
      assert(
        result
          .select(FIELD_1)
          .collect()
          .forall(
            _.getAs[java.util.Date](FIELD_1) == None.orNull
          ))
    }
  }

  test("cast to same type") {

    val content = Seq(
      Row(10L),
      Row(10L),
      Row(10L)
    )

    val df = createDataFrame(content, inputSchema(1, LongType))

    val dfResult = Formatter(
      field = FIELD_1,
      typeToCast = "long"
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      assert(result.schema.find(_.name == FIELD_1).get.dataType === LongType)
      getColumnAsSet(result, FIELD_1) shouldBe Set(10L)
    }
  }

  test("cast to Float with replacements") {

    val content = Seq(
      Row("A1.b"),
      Row("1.fe0"),
      Row("QQ1")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Formatter(
      field = FIELD_1,
      typeToCast = "float",
      replacements = Seq(
        Replacement("[a-zA-Z]", "")
      )
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      assert(result.schema.find(_.name == FIELD_1).get.dataType === FloatType)
      getColumnAsSet(result, FIELD_1) shouldBe Set(1.0f)
    }
  }

  test("cast to Date with replacements") {

    val content = Seq(
      Row("2017061221.36.31.172496"),
      Row("2017061221.36.31.172496")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Formatter(
      field = FIELD_1,
      typeToCast = "date",
      replacements = Seq(
        Replacement("(\\d{4})(\\d{2})(\\d{2})(.*)", "$1-$2-$3")
      )
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      assert(result.schema.find(_.name == FIELD_1).get.dataType === DateType)
      assert(
        result
          .select(FIELD_1)
          .collect()
          .forall(
            _.getAs[java.util.Date](FIELD_1) != None.orNull
          ))
    }
  }

  test("cast to None.orNullable Double with replacements") {

    val content = Seq(
      Row("1.2"),
      Row("1.2")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Formatter(
      field = FIELD_1,
      typeToCast = "double",
      replacements = Seq(
        Replacement(".*", "")
      )
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      assert(result.schema.find(_.name == FIELD_1).get.dataType === DoubleType)
      assert(
        result
          .select(FIELD_1)
          .collect()
          .forall(
            _.getAs[Double](FIELD_1) == None.orNull
          ))
    }
  }

  test("cast to Boolean with replacements") {

    val content = Seq(
      Row("11234Vadybn"),
      Row("1Nasd2")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = Formatter(
      field = FIELD_1,
      typeToCast = "boolean",
      replacements = Seq(
        Replacement("[0-9]", ""),
        Replacement("[a-z]", ""),
        Replacement("V", "True"),
        Replacement("N", "True")
      )
    ).transform(df)

    dfResult.isRight shouldBe true
    dfResult.map { result =>
      assert(result.schema.find(_.name == FIELD_1).get.dataType === BooleanType)

      assert(
        result
          .select(FIELD_1)
          .collect()
          .forall(
            _.getAs[Boolean](FIELD_1)
          ))
    }
  }
}
