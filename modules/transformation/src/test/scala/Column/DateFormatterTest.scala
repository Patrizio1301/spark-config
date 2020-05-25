package Column

import java.sql.Date
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import transformation.transformations.DateFormatter
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
class DateFormatterTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("check parse date correctly") {

    val format = "dd/MMM/yy"
    val sdf    = new SimpleDateFormat(format)

    val content = Seq(
      Row("31/Dic/16"),
      Row("01/Dic/16"),
      Row("01/Ene/17")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = DateFormatter(
      field = FIELD_1,
      format = format,
      operation = Some("parse"),
      locale = Some("es")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      result.select(FIELD_1).collect.map(_.getAs[Date](FIELD_1).toString).toSet shouldBe Set(
        "2016-12-31",
        "2016-12-01",
        "2017-01-01")
    }
  }

  test("check parse timestamp correctly") {
    Given("config")

    val format = "dd/MMM/yy HH:mm:ss"
    val content = Seq(
      Row("31/Dic/16 12:11:59"),
      Row("01/Dic/16 12:11:59"),
      Row("01/Ene/17 12:11:59")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))
    val dfResult = DateFormatter(
      field = FIELD_1,
      format = format,
      operation = Some("parseTimestamp"),
      locale = Some("es")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      result
        .select(FIELD_1)
        .collect
        .map(_.getAs[java.sql.Timestamp](FIELD_1).toString)
        .toSet shouldBe Set(
        "2016-12-31 12:11:59.0",
        "2016-12-01 12:11:59.0",
        "2017-01-01 12:11:59.0"
      )
    }
  }

  test("check parse timestamp correctly with milliseconds") {

    val format      = "dd/MMM/yy HH:mm:ss.SSSSSS"
    val dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
    val content = Seq(
      Row("31/Dic/16 12:11:59.333333"),
      Row("01/Dic/16 12:11:59.333333"),
      Row("01/Ene/17 12:11:59.444444")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = DateFormatter(
      field = FIELD_1,
      format = format,
      operation = Some("parseTimestamp"),
      locale = Some("es")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      result
        .select(FIELD_1)
        .collect
        .map(_.getAs[java.sql.Timestamp](FIELD_1))
        .map(
          _.toInstant
            .atZone(ZoneId
              .systemDefault())
            .toLocalDateTime())
        .map(_.format(dtFormatter))
        .toSet shouldBe Set("2016-12-31 " +
                              "12:11:59.333333",
                            "2016-12-01 12:11:59.333333",
                            "2017-01-01 12:11:59.444444")
    }
  }

  test("check format date correctly") {

    val format = "dd/MMM/yy"

    val content = Seq(
      Row("2016-12-31"),
      Row("2017-01-21")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = DateFormatter(
      field = FIELD_1,
      format = format,
      operation = Some("format"),
      locale = Some("es")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      result.select(FIELD_1).collect.map(_.getAs[String](FIELD_1)).toSet shouldBe Set("31/dic/16",
                                                                                      "21/ene/17")
    }
  }

  test("check format timestamp correctly") {

    val format = "dd/MMM/yy HH:mm:ss"

    val content = Seq(
      Row("2016-12-31 12:11:59.123"),
      Row("2017-01-21 12:11:59.123")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = DateFormatter(
      field = FIELD_1,
      format = format,
      operation = Some("format"),
      locale = Some("es")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      result.select(FIELD_1).collect.map(_.getAs[String](FIELD_1)).toSet shouldBe Set(
        "31/dic/16 12:11:59",
        "21/ene/17 12:11:59")
    }
  }

  test("check format timestamp correctly with milliseconds") {

    val format = "dd/MMM/yy HH:mm:ss.SSSSS"

    val content = Seq(
      Row("2016-12-31 12:11:59.123456"),
      Row("2017-01-21 12:11:59.123456")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = DateFormatter(
      field = FIELD_1,
      format = format,
      operation = Some("format"),
      locale = Some("es")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      result.select(FIELD_1).collect.map(_.getAs[String](FIELD_1)).toSet shouldBe Set(
        "31/dic/16 " +
          "12:11:59.12345",
        "21/ene/17 12:11:59.12345")
    }
  }

  test("check reformat date correctly") {

    val format   = "dd/MMM/yy HH:mm:ss"
    val reformat = "yyyy-MMM-dd HH:mm:ss"

    val content = Seq(
      Row("31/Dic/16 21:07:11"),
      Row("01/Dic/16 17:07:11")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = DateFormatter(
      field = FIELD_1,
      format = format,
      reformat = Some(reformat),
      operation = Some("reformat"),
      locale = Some("es_ES"),
      relocale = Some("de_DE")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe Set("2016-Dez-31 21:07:11", "2016-Dez-01 17:07:11")
    }
  }

  test("Reformat to extract hour") {
    Given("config")

    val format   = "dd/MM/yy HH:mm:ss"
    val reformat = "HH"

    val content = Seq(
      Row("31/12/16 21:07:11"),
      Row("01/12/16 17:07:11")
    )
    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = DateFormatter(
      field = FIELD_1,
      format = format,
      reformat = Some(reformat),
      operation = Some("reformat"),
      locale = Some("es")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      result.select(FIELD_1).collect.map(_.getAs[String](FIELD_1)).toSet shouldBe Set("21", "17")
    }
  }

  test("check parse date fail") {

    val format = "dd/MM/yy"

    val content = Seq(
      Row("010117"),
      Row("01 ENE 2017"),
      Row("01 01 17")
    )

    val df = createDataFrame(content, inputSchema(1, StringType))

    val dfResult = DateFormatter(
      field = FIELD_1,
      format = format,
      operation = Some("format")
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      result.select(FIELD_1).collect.map(_.getAs[Date](FIELD_1)).toSet shouldBe Set(None.orNull,
                                                                                    None.orNull,
                                                                                    None.orNull)
    }
  }
}
