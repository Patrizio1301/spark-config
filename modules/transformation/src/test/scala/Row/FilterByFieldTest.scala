package Row






//
//import java.sql.Date
//import java.util.Calendar
//
//import utils.test._
//import utils.test.schema.SchemaModels._
//import utils.test.schema.SchemaUtils.MetaData
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types._
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{GivenWhenThen, Matchers}
//import cats.implicits._
//import transformation.transformation.transformations.{Filter, FilterByField}
//import transformation.Transformations._
//import transformation.Transform._
//import transformation.transformation.transformations.Filter
//
//@RunWith(classOf[JUnitRunner])
//class FilterByFieldTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {
//
//  val caseTestWithoutNulls = Seq(
//    Row("Antonio", 12),
//    Row("Samuel", 1),
//    Row("Maria", 1),
//    Row("Alvaro", 2),
//    Row("Antonio", 3)
//  )
//
//  val caseTestWithNulls = Seq(
//    Row("Antonio", 12),
//    Row("Samuel", 1),
//    Row("Maria", 1),
//    Row(null, 2),
//    Row("Antonio", 3)
//  )
//
//  feature("Check funcionalities") {
//    test("FilterByField try to filter when value is Antonio") {
//
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Antonio", field = FIELD_1, op = "eq")
//        )
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 2
//        getColumnAsSet(result, FIELD_1) shouldBe Set("Antonio")
//      }
//    }
//
//    test("FilterByField try to filter when value is Maria") {
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Maria", field = FIELD_1, op = "eq")
//        )
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 1
//        getColumnAsSet(result, FIELD_1) shouldBe Set("Maria")
//      }
//    }
//
//    test("FilterByField try to filter when value is Maria with nulls") {
//
//      val dfwithNulls =
//        createDataFrame(caseTestWithNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Maria", field = FIELD_1, op = "eq")
//        )
//      ).transform(dfwithNulls)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 1
//        getColumnAsSet(result, FIELD_1) shouldBe Set("Maria")
//      }
//    }
//
//    test("FilterByField try to filter when value is Mariano") {
//
//      val dfwithNulls =
//        createDataFrame(caseTestWithNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Mariano", field = FIELD_1, op = "eq")
//        )
//      ).transform(dfwithNulls)
//
//      result.isRight shouldBe true
//
//      result.map(_.count() shouldBe 0)
//    }
//
//    test("FilterByField try to filter when value is Antonio for not equals") {
//
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Antonio", field = FIELD_1, op = "neq")
//        )
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 3
//        getColumnAsSet(result, FIELD_1) shouldBe Set("Samuel", "Maria", "Alvaro")
//      }
//    }
//
//    test("FilterByField try to filter when value is Antonio for lt") {
//
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Antonio", field = FIELD_1, op = "lt")
//        )
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 1
//        getColumnAsSet(result, FIELD_1) shouldBe Set("Alvaro")
//      }
//    }
//
//    test("FilterByField try to filter when value is Antonio for leq") {
//
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Antonio", field = FIELD_1, op = "leq")
//        )
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 3
//        getColumnAsSet(result, FIELD_1) shouldBe Set("Antonio", "Alvaro")
//      }
//    }
//
//    test("FilterByField try to filter when value is Antonio for gt") {
//
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Antonio", field = FIELD_1, op = "gt")
//        )
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 2
//        result.first().getAs[String](s"$FIELD_1") shouldBe "Samuel"
//      }
//    }
//
//    test("FilterByField try to filter when value is Antonio for geq") {
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Antonio", field = FIELD_1, op = "geq")
//        )
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 4
//        getColumnAsSet(result, FIELD_1) shouldBe Set("Antonio", "Samuel", "Maria")
//      }
//    }
//
//    test("FilterByField try to filter when value is Antonio for like") {
//
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "A%o", field = FIELD_1, op = "like")
//        )
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 3
//        getColumnAsSet(result, FIELD_1) shouldBe Set("Antonio", "Alvaro")
//      }
//    }
//
//    test("FilterByField try to filter when value is Antonio for rlike") {
//
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "A.*o", field = FIELD_1, op = "rlike")
//        )
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 3
//        getColumnAsSet(result, FIELD_1) shouldBe Set("Antonio", "Alvaro")
//      }
//    }
//
//    test("FilterByField try to filter when value is Antonio for an unsupported op") {
//
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Antonio", field = FIELD_1, op = "wololoo")
//        )
//      ).transform(dfwithoutNulls)
//
//      result.isLeft shouldBe true
//
//      result.leftMap { result =>
//        result.toString shouldBe "Error in transformation FilterByField: The paramater op has the invalid value wololoo. "
//      }
//    }
//
//    test("FilterByField with some filters AND") {
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Antonio", field = FIELD_1, op = "neq"),
//          Filter(value = "Samuel", field = FIELD_1, op = "neq")
//        ),
//        logicOp = Some("and")
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map(_.count() shouldBe 2)
//    }
//
//    test("FilterByField with some filters OR") {
//      val dfwithoutNulls =
//        createDataFrame(caseTestWithoutNulls, simpleSchema(Seq(StringType, IntegerType)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "Antonio", field = FIELD_1, op = "eq"),
//          Filter(value = "Samuel", field = FIELD_1, op = "eq")
//        ),
//        logicOp = Some("or")
//      ).transform(dfwithoutNulls)
//
//      result.isRight shouldBe true
//
//      result.map(_.count() shouldBe 3)
//    }
//
//    test("FilterByField try to filter when value is 2017-07-14") {
//
//      val calendar: Calendar       = Calendar.getInstance()
//      val dateToTest: Date         = new Date(calendar.getTime.getTime)
//      val columnToParse: List[Row] = Row(dateToTest) :: Row(dateToTest) :: Row(dateToTest) :: Nil
//      val df                       = createDataFrame(columnToParse, schemaSimple("date", DateType, MetaData()))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "2000-07-14", field = "date", op = "gt")
//        )
//      ).transform(df)
//
//      result.isRight shouldBe true
//
//      result.map(_.count() shouldBe 3)
//    }
//  }
//
//  feature("Check nested Structures") {
//
//    test("with nested structure") {
//      val content = Seq(
//        Row(Row(Row("dd"), 1, 2), "BB", "AA"),
//        Row(Row(Row("dd"), 1, 2), "BB", "AA"),
//        Row(Row(Row("dd"), 2, 3), "BB", "AA")
//      )
//      val df = createDataFrame(content,
//                               nestedSchema(Seq(StringType, IntegerType, StringType), Seq(3, 3, 1)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "1", field = s"$FIELD_1.$FIELD_2", op = "eq")
//        )
//      ).transform(df)
//
//      result.isRight shouldBe true
//
//      result.map(_.count() shouldBe 2)
//    }
//
//    test("with nested structure nivel 2") {
//      val content = Seq(
//        Row(Row(Row("aa"), 1, 2), "BB", "AA"),
//        Row(Row(Row("dd"), 1, 2), "BB", "AA"),
//        Row(Row(Row("dd"), 2, 3), "BB", "AA")
//      )
//      val df = createDataFrame(content,
//                               nestedSchema(Seq(StringType, IntegerType, StringType), Seq(3, 3, 1)))
//
//      val result = FilterByField(
//        filters = Seq(
//          Filter(value = "dd", field = s"$FIELD_1.$FIELD_1.$FIELD_1", op = "eq")
//        )
//      ).transform(df)
//
//      result.isRight shouldBe true
//
//      result.map { result =>
//        result.count() shouldBe 2
//      }
//    }
//  }
//}
