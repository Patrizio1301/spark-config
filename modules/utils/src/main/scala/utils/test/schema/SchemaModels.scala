package utils.test.schema

import utils.test.schema.SchemaGenerator._
import org.apache.spark.sql.types._
import utils.test.schema.SchemaUtils.MetaData

object SchemaModels {

  final val FIELD_1  = "A"
  final val FIELD_2  = "B"
  final val FIELD_3  = "C"
  final val FIELD_4  = "D"
  final val FIELD_5  = "E"
  final val FIELD_6  = "F"
  final val FIELD_7  = "G"
  final val FIELD_8  = "H"
  final val FIELD_9  = "I"
  final val FIELD_10 = "J"
  final val FIELD_11 = "K"
  final val FIELD_12 = "L"
  final val FIELD_13 = "M"
  final val FIELD_14 = "N"
  final val FIELD_15 = "O"
  final val FIELD_16 = "P"
  final val FIELD_17 = "Q"
  final val FIELD_18 = "R"
  final val FIELD_19 = "S"
  final val FIELD_20 = "T"
  final val FIELD_21 = "U"
  final val FIELD_22 = "V"
  final val FIELD_23 = "W"
  final val FIELD_24 = "X"
  final val FIELD_25 = "Y"
  final val FIELD_26 = "Z"

  def simpleSchema(
      dataType: Seq[DataType],
      name: Seq[String] = Seq(),
      metaData: Seq[MetaData] = Seq(),
      n: Int = 0
  ): StructType = {
    FlatSchema(dataType, name, metaData, n).build()
  }

  /**
    * Given the code inputSchema(5, StringType),
    * the input.output would be:
    *
    * StructType(Seq(
    *   StructField("A", StringType, true),
    *   StructField("B", StringType, true),
    *   StructField("C", StringType, true),
    *   StructField("D", StringType, true),
    *   StructField("E", StringType, true)
    * ))
    */
  val inputSchema = (n: Int, dataType: DataType) => simpleSchema(Seq(dataType), n = n)

  /**
    * Given the code schemaSimple("name", StringType, MetaData(optional=Some(false))),
    * the input.output would be:
    *
    * StructType(Seq(
    *   StructField("name", StringType, true, metaData(optional=false))
    * ))
    */
  val schemaSimple = (name: String, dataType: DataType, metadata: MetaData) =>
    simpleSchema(Seq(dataType), Seq(name), Seq(metadata))

  /**
    * Given the code EvolutionTypeschema(Seq(IntegerType, StringType, IntegerType)),
    * the input.output would be:
    *
    * StructType(Seq(
    *   StructField("member1", IntegerType, true),
    *   StructField("member2", StringType, true),
    *   StructField("member3", IntegerType, true)
    * ))
    */
  val EvolutionTypeschema = (dataType: Seq[DataType]) =>
    simpleSchema(dataType, name = (0 to 99).take(dataType.length).map(x => s"member${x.toString}"))

  /**
    * Given the code NestedTwoLevels(2, 2),
    * the input.output would be:
    *
    * StructType(Seq(
    *   StructField("A", StructType(Seq(
    *      StructField("A", StringType, true),
    *      StructField("B", StringType, true),
    *   )), true),
    *   StructField("B", StringType, true)
    * ))
    */
  val NestedTwoLevels = (numberA: Int, numberB: Int) =>
    simpleSchema(Seq(StringType), n = numberA)
      .sub(FIELD_1, FlatSchema(Seq(StringType), n = numberB).build())

  /**
    * Given the code nestedSchema(Seq(StringType, IntegerType, DoubleType),Seq(3,2,1))
    * the input.output would be:
    *
    * StructType(Seq(
    *   StructField("A", StructType(Seq(
    *      StructField("A",StructType(Seq(
    *         StructField("A", DoubleType, true)
    *      )), true),
    *      StructField("B", IntegerType, true),
    *   )), true),
    *   StructField("B", StringType, true),
    *   StructField("C", StringType, true),
    * ))
    */
  val nestedSchema = (dataTypes: Seq[DataType], n: Seq[Int]) =>
    simpleSchema(Seq(dataTypes(0)), n = n(0)).sub(
      FIELD_1,
      FlatSchema(Seq(dataTypes(1)), n = n(1))
        .build()
        .sub(FIELD_1, FlatSchema(Seq(dataTypes(2)), n = n(2))))

  val outputSchemaColumnTransformation = simpleSchema(Seq(StringType), n = 1).sub(
    FIELD_1,
    FlatSchema(Seq(StringType), n = 1)
      .build()
      .sub(FIELD_1, FlatSchema(Seq(StringType, IntegerType), n = 2)))

}
