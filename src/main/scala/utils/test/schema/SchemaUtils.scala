package utils.test.schema

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

object SchemaUtils {

  case class Schema(fields: SchemaField*)

  implicit class Schemas(schema: Schema) {

    def build(): StructType =
      StructType(schema.fields.map(field => schemaFieldConstructor(field)))
  }

  case class SchemaField(
      name: String,
      dataType: DataType,
      nullable: Boolean = true,
      metadata: MetaData = MetaData()
  )

  case class MetaData(
      default: Option[String] = None,
      optional: Option[Boolean] = None,
      logicalFormat: Option[String] = None,
      format: Option[String] = None,
      locale: Option[String] = None,
      deleted: Option[Boolean] = None,
      metaData: Option[Boolean] = None,
      rename: Option[String] = None,
      tokenized: Option[String] = None,
      tokenizationMethod: Option[String] = None
  )

  def schemaFieldConstructor(field: SchemaField): StructField = {
    StructField(
      field.name,
      field.dataType,
      field.nullable,
      getMetadata(field.metadata).build()
    )
  }

  def getMetadata(metadata: MetaData): MetadataBuilder = {
    val metadataBuilder = new MetadataBuilder()
    metadata.default.map(metadataBuilder.putString("default", _))
    metadata.optional.map(metadataBuilder.putBoolean("optional", _))
    metadata.logicalFormat.map(metadataBuilder.putString("logicalFormat", _))
    metadata.format.map(metadataBuilder.putString("format", _))
    metadata.locale.map(metadataBuilder.putString("locale", _))
    metadata.logicalFormat.map(metadataBuilder.putString("logicalFormat", _))
    metadata.rename.map(metadataBuilder.putString("rename", _))
    metadata.tokenized.map(metadataBuilder.putString("tokenized", _))
    metadata.tokenizationMethod.map(metadataBuilder.putString("tokenizationMethod", _))
    metadata.metaData.map(metadataBuilder.putBoolean("metaData", _))
    metadata.deleted.map(metadataBuilder.putBoolean("deleted", _))
    metadataBuilder
  }

  def createArrayType(schema: Schema, name: String = "arrayField"): StructType = {
    StructType(Seq(StructField(name, ArrayType(schema.build()))))
  }

  def createMapType(schema: Schema, dataType: DataType, name: String = "mapField"): StructType = {
    StructType(Seq(StructField(name, MapType(dataType, schema.build()))))
  }

  def createStructType(schema: Schema,
                       dataType: DataType,
                       name: String = "mapField"): StructType = {
    StructType(Seq(StructField(name, schema.build())))
  }
}
