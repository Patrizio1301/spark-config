package Column

import java.math.BigInteger
import java.security.MessageDigest

import cats.implicits._
import transformation.transformations.Hash
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import transformation.Transformations._
import transformation.Transform._

@RunWith(classOf[JUnitRunner])
class HashTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  private val content = Seq(
    Row("Jason", 1, null),
    Row("Benny Boy", 0, null),
    Row("Brendo Scrum", 3000, "The Skoberoi")
  )

  private val contentData = List(
    ("Jason", 1, null),
    ("Benny Boy", 0, null),
    ("Brendo Scrum", 3000, "The Skoberoi")
  )

  private val md5er    = MessageDigest.getInstance("MD5")
  private val sha1er   = MessageDigest.getInstance("SHA-1")
  private val sha256er = MessageDigest.getInstance("SHA-256")

  test("Run an md5 hash on a column") {
    val df: DataFrame =
      createDataFrame(content, simpleSchema(Seq(StringType, IntegerType, StringType)))

    When("applying the transformation")
    val dfResult = Hash(
      field = FIELD_1,
      hashType = "md5"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe contentData
        .map(row => new String(new BigInteger(1, md5er.digest(row._1.getBytes)).toString(16)))
        .toSet
    }
  }

  test("Run a sha-1 hash on a column") {
    val df: DataFrame =
      createDataFrame(content, simpleSchema(Seq(StringType, IntegerType, StringType)))

    val dfResult = Hash(
      field = FIELD_1,
      hashType = "sha1"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe contentData
        .map(row => new String(new BigInteger(1, sha1er.digest(row._1.getBytes)).toString(16)))
        .toSet
    }
  }

  test("Run a sha-256 hash on a column") {
    val df: DataFrame =
      createDataFrame(content, simpleSchema(Seq(StringType, IntegerType, StringType)))

    val dfResult = Hash(
      field = FIELD_1,
      hashType = "sha2",
      hashLength = Some(256)
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_1) shouldBe contentData
        .map(row => new String(new BigInteger(1, sha256er.digest(row._1.getBytes)).toString(16)))
        .toSet
    }
  }

  test("Run an md5 hash on an integer column") {
    val df: DataFrame =
      createDataFrame(content, simpleSchema(Seq(StringType, IntegerType, StringType)))

    val dfResult = Hash(
      field = FIELD_2,
      hashType = "md5"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_2) shouldBe contentData
        .map(row =>
          new String(new BigInteger(1, md5er.digest(row._2.toString.getBytes)).toString(16)))
        .toSet
    }
  }

  test("Run an md5 hash on a column with nulls") {
    val df: DataFrame =
      createDataFrame(content, simpleSchema(Seq(StringType, IntegerType, StringType)))

    val dfResult = Hash(
      field = FIELD_3,
      hashType = "md5"
    ).transform(df)

    dfResult.isRight shouldBe true

    dfResult.map { result =>
      getColumnAsSet(result, FIELD_3) shouldBe contentData
        .map(row =>
          if (row._3 == null) {
            null
          } else {
            new String(new BigInteger(1, md5er.digest(row._3.getBytes)).toString(16))
        })
        .toSet
    }
  }

  test("Run an md5 hash on a column with an invalid hashType") {
    val df: DataFrame =
      createDataFrame(content, simpleSchema(Seq(StringType, IntegerType, StringType)))

    val dfResult = Hash(
      field = FIELD_3,
      hashType = "AES"
    ).validate.map(op => op.transform(df))

    dfResult.isLeft shouldBe true

    dfResult.leftMap { error =>
      error.toString() shouldBe "Error in transformation Hash: The paramater hashType has the invalid value AES. Valid values are: md5, sha1, sha2."
    }
  }

  test("Run a sha2 hash on a column with an invalid hashlength") {
    val df: DataFrame =
      createDataFrame(content, simpleSchema(Seq(StringType, IntegerType, StringType)))

    assertThrows[java.lang.IllegalArgumentException] {
      Hash(
        field = FIELD_3,
        hashType = "sha2",
        hashLength = Some(420)
      ).transform(df)
    }
  }
}
