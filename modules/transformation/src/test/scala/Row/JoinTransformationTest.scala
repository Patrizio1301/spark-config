package Row







//import utils.test.InitSparkSessionFunSuite
//import transformation.transformation.transformations.row._
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.Row
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{GivenWhenThen, Matchers}
//import cats.implicits._
//import transformation.Transformations._
//import transformation.Transform._
//import transformation.transformation.transformations.{JoinColumn, JoinConfig, JoinTransformation}
//
//@RunWith(classOf[JUnitRunner])
//class JoinTransformationTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {
//
//  val schema = StructType(
//    StructField("text", StringType) ::
//      StructField("dropColumn", StringType) :: Nil
//  )
//
//  feature("Check functionalities") {
//    test("join columns correctly with one table") {
//      import spark.implicits._
//      val df = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
//
//      val join = JoinTransformation(
//        joins = Seq(
//          JoinConfig(
//            inputDf = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j(strange)j", "z"),
//            alias = "t1",
//            joinType = "left",
//            joinColumns = Seq(JoinColumn(self = "x", other = "x"))
//          )),
//        resolveConflictsAuto = true,
//        select = Seq("self.x", "self.y", "self.z", "t1.z", "t1.`j(strange)j` as j")
//      ).transform(df)
//
//      join.isRight shouldBe true
//
//      join.map { result =>
//        result.columns.toSet shouldBe Set("x", "t1_z", "j", "y", "z")
//
//        val expectedResult = Set(Row(1, 2, 3, 2, 3),
//                                 Row(1, 1, 3, 2, 3),
//                                 Row(2, 2, 3, 2, 3),
//                                 Row(1, 1, 3, 1, 3),
//                                 Row(1, 2, 3, 1, 3))
//
//        result.collect.map(row => row).toSet shouldBe expectedResult
//        result.columns.toList
//          .groupBy((k) => k)
//          .filter({ case (_, l) => l.size > 1 })
//          .keySet shouldBe Set()
//      }
//    }
//
////    test("join columns correctly with one table applying transformation.transformations") {
////
////      import spark.implicits._
////      val df = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
////
////      val join = Jointransformation(
////        joins = Seq(
////          JoinConfig(
////            inputDf = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j(strange)j", "z"),
////            alias = "t1",
////            joinType = "left",
////            joinColumns = Seq(JoinColumn(self = "x", other = "x"))
////            )
////          ),
////        resolveConflictsAuto = true,
////        select = Seq("self.x", "self.y", "self.z", "t1.z", "t1.`j(strange)j` as j")
////      ).transform(df)
////
////      join.isRight shouldBe true
////
////      join.map { result =>
////        result.columns.toSet shouldBe Set("x", "t1_z", "j", "y", "z")
////
////        val expectedResult =
////          Set(Row(1, 1, 3, null, null), Row(1, 2, 3, null, null), Row(2, 2, 3, 2, 3))
////        result.collect.map(row => row).toSet shouldBe expectedResult
////        result.columns.toList
////          .groupBy((k) => k)
////          .filter({ case (_, l) => l.size > 1 })
////          .keySet shouldBe Set()
////      }
////    }
//
//    test("join columns correctly with two tables") {
//      import spark.implicits._
//      val df = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
//
//      val join = JoinTransformation(
//        joins = Seq(
//          JoinConfig(
//            inputDf = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z"),
//            alias = "t1",
//            joinType = "left",
//            joinColumns = Seq(JoinColumn(self = "x", other = "x"))
//          ),
//          JoinConfig(
//            inputDf = List((5, 6, 7), (7, 3, 3), (2, 2, 13)).toDF("x", "y", "z"),
//            alias = "t2",
//            joinType = "inner",
//            joinColumns = Seq(JoinColumn(self = "t1.y", other = "y"))
//          )
//        ),
//        resolveConflictsAuto = true,
//        select = Seq("self.x",
//                     "self.y",
//                     "self.z",
//                     "t1.x as x1",
//                     "t1.y as y1",
//                     "t1.z as z1",
//                     "t2.x as x2",
//                     "t2.y as y2",
//                     "t2.z as z2")
//      ).transform(df)
//
//      join.isRight shouldBe true
//
//      join.map { result =>
//        result.columns.toSet shouldBe Set("x", "y", "z1", "x2", "z2", "y1", "x1", "z", "y2")
//
//        val expectedResult = Set(Row(1, 1, 3, 1, 2, 3, 2, 2, 13),
//                                 Row(1, 2, 3, 1, 2, 3, 2, 2, 13),
//                                 Row(2, 2, 3, 2, 2, 3, 2, 2, 13))
//        result.collect.map(row => row).toSet shouldBe expectedResult
//
//        result.columns.toList
//          .groupBy((k) => k)
//          .filter({ case (_, l) => l.size > 1 })
//          .keySet shouldBe Set()
//      }
//    }
//
////    test("join columns correctly with two tables applying transformation.transformations") {
////      Given("A configuration")
////      val config = ConfigFactory.parseString(
////        """
////          |  {
////          |    joins =
////          |      [
////          |        {
////          |          input = {
////          |            key=1
////          |          }
////          |          alias = t1
////          |          joinType = "left"
////          |          joinColumns = [
////          |            {
////          |              self= "x"
////          |              other="x"
////          |            }
////          |          ]
////          |          transformation.transformations = [
////          |            {
////          |              type = "formatter"
////          |              field = "z"
////          |              typeToCast = "string"
////          |              replacements: [{
////          |                pattern = "[0-9]"
////          |                replacement = "test"
////          |              }]
////          |            }
////          |          ]
////          |        },
////          |        {
////          |          input = {
////          |            key=2
////          |          }
////          |          alias = t2
////          |          joinType = "inner"
////          |          joinColumns = [
////          |            {
////          |              self= "t1.y"
////          |              other="y"
////          |            }
////          |          ]
////          |          transformation.transformations = [
////          |            {
////          |              type = "filter"
////          |              filters: [{
////          |                field = "x"
////          |                value = 2
////          |                op = "gt"
////          |              }]
////          |            },
////          |            {
////          |              type = "formatter"
////          |              field = "y"
////          |              typeToCast = "integer"
////          |              replacements: [{
////          |                pattern = "(\\d{4})-(\\d{2})-(\\d{2})"
////          |                replacement = "$1"
////          |              }]
////          |            }
////          |          ]
////          |        }
////          |      ]
////          |    resolveConflictsAuto = true
////          |    select = ["self.x","self.y", "self.z","t1.x as x1","t1.y as y1","t1.z as z1","t2.x as x2","t2.y as y2","t2.z as z2"]
////          |    type = "join"
////          |  }
////        """.stripMargin)
////
////      Given("A Input Dataframe")
////      import spark.implicits._
////      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
////
////      Given("Two join dataframes")
////      When("Perform the join")
////      val dfCleaned = new JoinTransformation(config) {
////        override def getInput(sparkSession: SparkSession,
////                              alias: String,
////                              input: Config): DataFrame = {
////          if (input.getInt("key") == 1) {
////            List((1, 2020, 3), (1, 2019, 3), (1, 2019, 3), (2, 2018, 3)).toDF("x", "y", "z")
////          } else {
////            List((9, "2020-07-16", 7),
////              (1, "2019-07-16", 7),
////              (3, "2018-07-16", 3),
////              (2, "2017-07-16", 13)).toDF("x", "y", "z")
////          }
////        }
////      }.transform(dfOrigin)
////
////      Then("result column names must be the selected columns")
////      dfCleaned.columns.toSet shouldBe Set("x", "y", "z1", "x2", "z2", "y1", "x1", "z", "y2")
////
////      Then("result dataframe must be the expected")
////      val expectedResult = Set(Row(1, 1, 3, 1, 2020, "test", 9, 2020, 7),
////        Row(1, 2, 3, 1, 2020, "test", 9, 2020, 7),
////        Row(2, 2, 3, 2, 2018, "test", 3, 2018, 3))
////      dfCleaned.collect.map(row => row).toSet shouldBe expectedResult
////
////      Then("There aren't column names repeated")
////      dfCleaned.columns.toList
////        .groupBy((k) => k)
////        .filter({ case (_, l) => l.size > 1 })
////        .keySet shouldBe Set()
////    }
//
//    test("join columns correctly with one table and without 'select'") {
//
//      import spark.implicits._
//      val df = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
//
//      val join = JoinTransformation(
//        joins = Seq(
//          JoinConfig(
//            inputDf = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z"),
//            alias = "t1",
//            joinType = "left",
//            joinColumns = Seq(JoinColumn(self = "x", other = "x"))
//          )
//        ),
//        resolveConflictsAuto = true
//      ).transform(df)
//
//      join.isRight shouldBe true
//
//      join.map { result =>
//        result.columns.toSet shouldBe Set("x", "t1_z", "j", "y", "t1_x", "z")
//        result.columns.toList
//          .groupBy((k) => k)
//          .filter({ case (_, l) => l.size > 1 })
//          .keySet shouldBe Set()
//      }
//    }
//
////    test("join columns correctly with self") {
////      import spark.implicits._
////      val df = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
////
////      Jointransformation(
////        joins = Seq(
////          JoinConfig(
////            inputDf = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z"),
////            alias = "me",
////            joinType = "left",
////            joinColumns = Seq(JoinColumn(self = "x", other = "x"))
////          )
////        ),
////        resolveConflictsAuto = true,
////        select = Seq("self.x", "self.y", "self.z", "me.x", "me.y", "me.z")
////      ).transform(df).map {
////        result =>
////          result.columns.toSet shouldBe Set("x", "me_x", "y", "me_z", "me_y", "z")
////
////          val expectedResult = Set(Row(2, 2, 3, 2, 2, 3),
////            Row(1, 2, 3, 1, 1, 3),
////            Row(1, 1, 3, 1, 1, 3),
////            Row(1, 1, 3, 1, 2, 3),
////            Row(1, 2, 3, 1, 2, 3))
////          result.collect.map(row => row).toSet shouldBe expectedResult
////
////          result.columns.toList
////            .groupBy((k) => k)
////            .filter({ case (_, l) => l.size > 1 })
////            .keySet shouldBe Set()
////      }
////    }
//
//    test("join columns correctly with one table and using * on select") {
//
//      import spark.implicits._
//      val df = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
//
//      val join = JoinTransformation(
//        joins = Seq(
//          JoinConfig(
//            inputDf = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z"),
//            alias = "t1",
//            joinType = "left",
//            joinColumns = Seq(JoinColumn(self = "x", other = "x"))
//          )
//        ),
//        resolveConflictsAuto = true,
//        select = Seq("t1.*", "self.*")
//      ).transform(df)
//
//      join.isRight shouldBe true
//
//      join.map { result =>
//        result.columns.toSet shouldBe Set("x", "t1_z", "j", "y", "t1_x", "z")
//
//        result.columns.toList
//          .groupBy((k) => k)
//          .filter({ case (_, l) => l.size > 1 })
//          .keySet shouldBe Set()
//      }
//    }
////
////    test("pass invalid configuration with 'joinColumns' empty") {
////      Given("A configuration")
////      val configJoinColumnsEmpty = ConfigFactory.parseString(
////        """
////          |{
////          |      joins = [ {
////          |        input = {}
////          |        alias = t1
////          |        joinType = "left"
////          |        joinColumns = []
////          |      }]
////          |      type = "join"
////          |    }
////        """.stripMargin)
////
////      Given("A Input Dataframe")
////      import spark.implicits._
////      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
////
////      Given("A join dataframe")
////      When("Perform the join")
////      Then("throws correct exception")
////      val caught = intercept[JoinTransformationException] {
////        new JoinTransformation(configJoinColumnsEmpty) {
////          override def getInput(sparkSession: SparkSession,
////                                alias: String,
////                                input: Config): DataFrame = {
////            List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z")
////          }
////        }.transform(dfOrigin)
////      }
////
////      caught.message shouldBe "JoinTransformation: 'joinColumns' cannot be empty"
////    }
//
//    test("pass invalid configuration with 'joinColumns' with invalid columns") {
//      import spark.implicits._
//      val df = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
//
//      val join = JoinTransformation(
//        joins = Seq(
//          JoinConfig(
//            inputDf = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z"),
//            alias = "t1",
//            joinType = "left",
//            joinColumns = Seq(JoinColumn(self = "x", other = "h"))
//          )
//        ),
//        resolveConflictsAuto = true
//      ).transform(df)
//
//      join.isLeft shouldBe true
//
//      join.leftMap { error =>
//        error.toString shouldBe "Error in transformation Join: " +
//          "The transformation caused the following unexpected error : 'joinColumns' contains unknown columns"
//      }
//
//    }
//
//    test("pass invalid configuration with 'joinColumns' with duplicated aliases") {
//      import spark.implicits._
//      val df = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
//
//      val join = JoinTransformation(
//        joins = Seq(
//          JoinConfig(
//            inputDf = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z"),
//            alias = "t1",
//            joinType = "left",
//            joinColumns = Seq(JoinColumn(self = "x", other = "x"))
//          ),
//          JoinConfig(
//            inputDf = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z"),
//            alias = "t1",
//            joinType = "inner",
//            joinColumns = Seq(JoinColumn(self = "t1.y", other = "y"))
//          )
//        ),
//        resolveConflictsAuto = true,
//        select = Seq("self.x", "self.y", "self.z", "t1.x", "t1.y", "t1.z", "t2.x", "t2.x", "t2.z")
//      ).transform(df)
//
//      join.isLeft shouldBe true
//
//      join.leftMap { e =>
//        e.toString shouldBe "Error in transformation Join: " +
//          "The transformation caused the following unexpected error : 'joinColumns' contains unknown columns"
//      }
//    }
//
//    test("pass invalid select with duplicated aliases") {
//
//      import spark.implicits._
//      val df = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
//
//      val join = JoinTransformation(
//        joins = Seq(
//          JoinConfig(
//            inputDf = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z"),
//            alias = "t1",
//            joinType = "left",
//            joinColumns = Seq(JoinColumn(self = "x", other = "x"))
//          )
//        ),
//        resolveConflictsAuto = true,
//        select = Seq("self.x as x1", "t1.x as x1")
//      ).transform(df).leftMap { e =>
//        e.toString shouldBe "Error in transformation Join: " +
//          "The transformation caused the following unexpected error : " +
//          "JoinTransformation: There are duplicated columns in input.output that need to be resolved manually: x1"
//      }
//    }
//
//    test("join columns correctly with one table using nested columns") {
//
//      import spark.implicits._
//      val df = List(((1, 1), 3), ((1, 2), 3), ((2, 2), 3)).toDF("x", "y")
//
//      val join = JoinTransformation(
//        joins = Seq(
//          JoinConfig(
//            inputDf = List((1, (1, 3)), (1, (2, 3)), (2, (2, 3))).toDF("x", "y"),
//            alias = "t1",
//            joinType = "left",
//            joinColumns = Seq(JoinColumn(self = "x._2", other = "y._1"))
//          )
//        ),
//        resolveConflictsAuto = true,
//        select = Seq("self.x._1 as main_x_1", "t1.*")
//      ).transform(df)
//
//      join.isRight shouldBe true
//
//      join.map { result =>
//        result.schema shouldBe StructType(
//          Seq(
//            StructField("main_x_1", IntegerType, true),
//            StructField("x", IntegerType, true),
//            StructField("y",
//                        StructType(Seq(StructField("_1", IntegerType, false),
//                                       StructField("_2", IntegerType, false))),
//                        true)
//          ))
//
//        val expectedResult = Set(Row(1, 1, Row(2, 3)),
//                                 Row(2, 2, Row(2, 3)),
//                                 Row(2, 1, Row(2, 3)),
//                                 Row(1, 1, Row(1, 3)),
//                                 Row(1, 2, Row(2, 3)))
//        result.collect.map(row => row).toSet shouldBe expectedResult
//
//        result.columns.toList
//          .groupBy((k) => k)
//          .filter({ case (_, l) => l.size > 1 })
//          .keySet shouldBe Set()
//      }
//    }
//  }
//}
