package org.example

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, IndexedRecord}
import org.apache.avro.reflect.ReflectData
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.{InitializeJavaBean, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{Expression, GetStructField, If, IsNull, Literal}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.example.SchemaConverters.toSqlType

import scala.reflect.ClassTag

object AvroExpressionEncoder {

  def of[T <: SpecificRecord](avroClass: Class[T]): AvroExpressionEncoder[T] = {
    val schema = avroClass.getMethod("getClassSchema").invoke(null).asInstanceOf[Schema]
    assert(toSqlType(schema).dataType.isInstanceOf[StructType])
    val serializer = AvroTypeInference.serializerFor(avroClass, schema)
    val deserializer = AvroTypeInference.deserializerFor(schema)
    new AvroExpressionEncoder[T](
      serializer,
      deserializer,
      ClassTag[T](avroClass))
  }

  def of[T <: IndexedRecord](schema: Schema): AvroExpressionEncoder[T] = {
    assert(toSqlType(schema).dataType.isInstanceOf[StructType])
    val avroClass = Option(ReflectData.get.getClass(schema))
      .map(_.asSubclass(classOf[SpecificRecord]))
      .getOrElse(classOf[GenericData.Record])
    val serializer = AvroTypeInference.serializerFor(avroClass, schema)
    val deserializer = AvroTypeInference.deserializerFor(schema)
    new AvroExpressionEncoder[T](
      serializer,
      deserializer,
      ClassTag[T](avroClass))
  }
}

class AvroExpressionEncoder[T](
                                   override val objSerializer: Expression,
                                   override val objDeserializer: Expression,
                                   override val clsTag: ClassTag[T])
  extends ExpressionEncoder[T] (objSerializer, objDeserializer, clsTag) {
  override val deserializer: Expression = {
    if (isSerializedAsStructForTopLevel) {
      // We serialized this kind of objects to root-level row. The input of general deserializer
      // is a `GetColumnByOrdinal(0)` expression to extract first column of a row. We need to
      // transform attributes accessors.
      objDeserializer.transform {
        case UnresolvedExtractValue(GetColumnByOrdinal(0, _),
        Literal(part: UTF8String, StringType)) =>
          UnresolvedAttribute.quoted(part.toString)
        case GetStructField(GetColumnByOrdinal(0, dt), ordinal, _) =>
          GetColumnByOrdinal(ordinal, dt)
        case If(IsNull(GetColumnByOrdinal(0, _)), _, n: NewInstance) => n
        case If(IsNull(GetColumnByOrdinal(0, _)), _, i: InitializeJavaBean) => i
        case If(IsNull(GetColumnByOrdinal(0, _)), _, i: InitializeAvroObject) => i
      }
    } else {
      // For other input objects like primitive, array, map, etc., we deserialize the first column
      // of a row to the object.
      objDeserializer
    }
  }
}
