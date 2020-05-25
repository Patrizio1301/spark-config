package Column

//@RunWith(classOf[JUnitRunner])
//class ColumnTransformationTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {
//
//  case class TestTransformation(field: String) extends ColumnTransformations {
//
//    override val fieldName: String = field
//
//    import org.apache.spark.sql.functions._
//
//    override def transform(col: Column): Either[TransformationError, Column] = lit(1).asRight
//  }
//
//  feature("test column transformation") {
//
//    test("test column create") {
//
//      val content = Seq(
//        Row("a"),
//        Row("b"),
//        Row("c")
//      )
//
//      val df = createDataFrame(content, inputSchema(1, StringType))
//
//      val result = TestTransformation(field = FIELD_2).transform(df)
//
//      result.isRight shouldBe true
//
//      result.map { df =>
//        getColumnAsSet(df, FIELD_2) shouldBe Set(1)
//      }
//    }
//
//    test("test column update") {
//
//      val content = Seq(
//        Row("a"),
//        Row("b"),
//        Row("c")
//      )
//
//      val df = createDataFrame(content, inputSchema(1, StringType))
//
//      val result = TestTransformation(field = "test").transform(df)
//
//      result.isRight shouldBe true
//
//      result.map { df =>
//        getColumnAsSet(df, "test") shouldBe Set(1)
//      }
//    }
//
//    test("test nested column update") {
//
//      val content = Seq(Row(Row(Row("z"))))
//
//      val df = createDataFrame(content,
//                               nestedSchema(Seq(StringType, StringType, StringType), Seq(1, 1, 1)))
//
//      val result = TestTransformation(field = s"$FIELD_1.$FIELD_1.$FIELD_1").transform(df)
//      result.isRight shouldBe true
//
//      result.map { df =>
//        df.schema shouldBe nestedSchema(Seq(StringType, StringType, IntegerType), Seq(1, 1, 1))
//        getColumnAsSet(df, s"$FIELD_1.$FIELD_1.$FIELD_1") shouldBe Set(1)
//      }
//    }
//
//    test("test nested column update 2") {
//      val content = Seq(Row(Row(Row("z"))))
//
//      val df = createDataFrame(content,
//                               nestedSchema(Seq(StringType, StringType, StringType), Seq(1, 1, 1)))
//
//      val result = TestTransformation(field = s"$FIELD_1.$FIELD_1.$FIELD_2").transform(df)
//
//      result.isRight shouldBe true
//
//      result.map { df =>
//        df.schema shouldBe outputSchemaColumnTransformation
//        getColumnAsSet(df, s"$FIELD_1.$FIELD_1.$FIELD_1") shouldBe Set("z")
//        getColumnAsSet(df, s"$FIELD_1.$FIELD_1.$FIELD_2") shouldBe Set(1)
//      }
//    }
//  }
//}
