package utils.implicits

import org.apache.spark.sql.types._

object StructTypes {

  implicit class ImplicitFunctions(schema: StructType) extends Serializable {

    /**
      * This function unstructures a nested schema into a unnested schema.
      *
      * @return unnested schema.
      */
    def unnested(): StructType = {
      StructType(unnestedRecursive("", schema.fields))
    }

    /**
      * Given a data type, this method returns the same datatype but without sub-structure if apply.
      *
      * @param x DataStructure
      * @return DataType
      */
    private def dataTypeSetting[T <: DataType](x: T): DataType = {
      x match {
        case a: MapType      => MapType(a.keyType, a.valueType)
        case _: ArrayType    => ArrayType(StructType(Seq()))
        case _: StructType   => StructType(Seq())
        case other: DataType => other
      }
    }

    private def inheritedDataType[T <: DataType](x: T): DataType = {
      x match {
        case m: MapType      => m.valueType
        case a: ArrayType    => a.elementType
        case other: DataType => other
      }
    }

    private def fieldCopyDataType[T <: DataType](x: T): DataType = {
      (dataTypeSetting _).compose(inheritedDataType)(x)
    }

    /**
      * Given a field, with new name and new data type, this method copies/transforms the given structField by
      * A) changing the name to the value of the parameter *name*.
      * B) Changing the dataType to the given data type in the parameter *datatype*,
      * taking into account that the field.dataType structure must be maintained.
      *
      * @param field    Field
      * @param name     New field name
      * @param dataType DataType to apply
      * @return Transformed structField
      */
    private def fieldTransformation[T <: DataType](field: StructField,
                                                   name: String,
                                                   dataType: T): StructField = {
      field.dataType match {
        case m: MapType      => field.copy(name = name, dataType = MapType(m.keyType, dataType))
        case _: ArrayType    => field.copy(name = name, dataType = ArrayType(dataType))
        case _: StructType   => field.copy(name = name, dataType = dataType)
        case other: DataType => field.copy(name = name, dataType = other)
      }
    }

    /**
      * Given a field name, its datatype and a function, this method identifies substructures. If the datatype x is:
      * A) MapType => Same function will be applied but to the valueType (in valuetype must be the children).
      * B) ArrayType => Same function will be applied but to the elementType (in elementType must be the children).
      * C) StructType => The parameter function *func* will be applied to this structure in order to identify each child.
      * D) In all other cases no children are detected and just a empty sequence will be returned.
      *
      * @param name Field name
      * @param x    DataStructure
      * @param func Function to apply if field structure is structType
      * @return Sequence of sub-structFields
      */
    def inheritedComplexType[T <: DataType](
                                             name: String,
                                             x: T,
                                             func: (String, Seq[StructField]) => Seq[StructField]): Seq[StructField] = {
      x match {
        case a: MapType    => inheritedComplexType(name, a.valueType, func)
        case a: ArrayType  => inheritedComplexType(name, a.elementType, func)
        case a: StructType => func(name, a.fields)
        case _             => Seq()
      }
    }

    /**
      * This recursive method takes a nested field and its children and adds each child
      * to a sequence by applying to each child:
      * A) Change the name to "parent_name.chile_name" if parents is non empty, else "child_name".
      * B) Change dataType by eliminating substructures.
      * C) Maintains all other attributes (metadatas, nullable).
      *
      * After adding each child, if a child has a complex type, each of its children has to be added. This is done
      * recursively by the method *inheritedComplexType*.
      *
      * @param parent Field name of nested field with at least one child
      * @param schema Sequence of fields
      * @return Sequence of fields
      */
    private def unnestedRecursive(parent: String, schema: Seq[StructField]): Seq[StructField] = {
      schema.foldLeft(Seq[StructField]()) { (plainSchema, element) =>
        val name     = s"${if (parent.isEmpty) "" else s"$parent."}${element.name}"
        val newField = Seq(fieldTransformation(element, name, fieldCopyDataType(element.dataType)))
        plainSchema ++ newField ++ inheritedComplexType(name, element.dataType, unnestedRecursive)
      }
    }

    /**
      * Given an unnested schema, this methods transforms this schema into a nested schema
      * in compliance with the field names.
      *
      * @return nested schema
      */
    def nested(): StructType = {
      schema.fields match {
        case Array() => StructType(Seq())
        case _ =>
          val min = schema.fields.map(_.name.split('.').length).min - 1
          StructType(nestedRecursive(schema.fields, min))
      }

    }

    /**
      * This methods identifies all children of field and returns a sequence of all this children.
      *
      * @param schema Schema
      * @param field  Parent field
      * @return Sequence of all children.
      */
    private def getAllSubFields(schema: Seq[StructField], field: String): Seq[StructField] = {
      schema.filter(_.name.startsWith(field + "."))
    }

    /**
      * Given a schema and a level of inheritance (number of parents), this method takes all fields of level at most
      * *level*. For each of these fields he select the child name (without namespace),
      * and created a copy of the structField by doing:
      * A) If the field has no children =>  created a copy of the structField and just change the name.
      * B) If the field has children =>
      * 1) name and datatype changes (field transformation will be done by *fieldtransformation*),
      * 2) Data type is determined by the substructure which will be identified recursively.
      *
      * @param schema Schema
      * @param level  Number of parents
      * @return Sequence of structfields with nested structure
      */
    private def nestedRecursive(schema: Seq[StructField], level: Int): Seq[StructField] = {
      schema.filter(_.name.count(_ == '.') <= level).foldLeft(Seq[StructField]()) { (seq, elem) =>
        val name: String = elem.name.split('.').last
        if (getAllSubFields(schema, elem.name).isEmpty) {
          seq ++ Seq(elem.copy(name = name))
        } else {
          seq ++ Seq(
            fieldTransformation(
              elem,
              name,
              StructType(nestedRecursive(getAllSubFields(schema, elem.name), level + 1)))
          )
        }
      }
    }

    /**
      * Given a field name, this method identifies all its brothers.
      *
      * @param field Field whose brothers are required.
      * @return Schema with brothers of *field*.
      */
    def getBrothers(field: String): Seq[StructField] = {
      val parent = field.split('.').dropRight(1).mkString(".")
      val level  = field.split('.').length - 1
      StructType(
        schema
          .unnested()
          .filter(x =>
            x.name.startsWith(parent) && !x.name
              .startsWith(field) && x.name.split('.').length - 1 >= level))
        .nested()
    }

  }
}