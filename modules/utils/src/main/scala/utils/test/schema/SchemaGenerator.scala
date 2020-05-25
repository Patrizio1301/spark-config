package utils.test.schema

import utils.test.schema.SchemaUtils.{MetaData, Schema, SchemaField}
import org.apache.spark.sql.types.{DataType, StructType}
import utils.test.schema.SchemaUtils.MetaData

object SchemaGenerator {
  implicit def flatSchemaToStructType(fs: FlatSchema): StructType = fs.build()

  val letters = 'A' to 'Z'

  case class FlatSchema(
      dataType: Seq[DataType],
      name: Seq[String] = Seq(),
      metaData: Seq[MetaData] = Seq(),
      n: Int = 0
  )

  implicit class FlatSchemaFunctions(fs: FlatSchema) {
    def build(): StructType = {
      val num = fs.n match {
        case 0 => Seq(fs.name.length, fs.dataType.length, fs.metaData.length).max
        case _ => fs.n
      }
      val names: Seq[String] = fs.name match {
        case Seq()                      => letters.take(num).map(_.toString)
        case _ if fs.name.length == num => fs.name
        case _ =>
          throw new IllegalArgumentException(
            s"Schema generator: The sequence of ${fs.name.mkString(", ")} must be of length $num.")
      }
      val metaDatas: Seq[MetaData] = fs.metaData match {
        case Seq()                          => Seq.fill(num)(MetaData())
        case _ if fs.metaData.length == num => fs.metaData
        case _ =>
          throw new IllegalArgumentException(
            s"Schema generator: The sequence of ${fs.metaData.mkString(", ")} must be of length $num.")
      }
      val dataTypes: Seq[DataType] = fs.dataType match {
        case _ if fs.dataType.length == num => fs.dataType
        case _ if fs.dataType.length == 1   => Seq.fill(num)(fs.dataType.head)
        case _ =>
          throw new IllegalArgumentException(
            s"Schema generator: The sequence of ${fs.dataType.mkString(", ")} must be of length $num.")
      }
      Schema(
        (names, dataTypes, metaDatas).zipped.toSeq.map(x => SchemaField(x._1, x._2, true, x._3)): _*
      ).build()
    }
  }
  implicit class StructTypeFunctions(st: StructType) {

    def sub(name: String, schema: DataType): StructType = {
      StructType(
        st.fields.map(field =>
          field.name match {
            case _name: String if _name == name => field.copy(dataType = schema)
            case _                              => field
        })
      )
    }

  }
}
