package org.example

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.avro.LogicalTypes.{Date, Decimal, TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.spark.sql.types.Decimal.minBytesForPrecision
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
 * This object contains method that are used to convert sparkSQL schemas to avro schemas and vice
 * versa.
 */
object SchemaConverters {

  private lazy val nullSchema = Schema.create(Schema.Type.NULL)

  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
   * This function takes an avro schema and returns a sql schema.
   */
  def toSqlType(avroSchema: Schema): SchemaType = {
    toSqlTypeHelper(avroSchema, Set.empty)
  }

  def toSqlTypeHelper(avroSchema: Schema, existingRecordNames: Set[String]): SchemaType = {
    avroSchema.getType match {
      case INT => avroSchema.getLogicalType match {
        case _: Date => SchemaType(DateType, nullable = false)
        case _ => SchemaType(IntegerType, nullable = false)
      }
      case STRING => SchemaType(StringType, nullable = false)
      case BOOLEAN => SchemaType(BooleanType, nullable = false)
      case BYTES | FIXED => avroSchema.getLogicalType match {
        // For FIXED type, if the precision requires more bytes than fixed size, the logical
        // type will be null, which is handled by Avro library.
        case d: Decimal => SchemaType(DecimalType(d.getPrecision, d.getScale), nullable = false)
        case _ => SchemaType(BinaryType, nullable = false)
      }

      case DOUBLE => SchemaType(DoubleType, nullable = false)
      case FLOAT => SchemaType(FloatType, nullable = false)
      case LONG => avroSchema.getLogicalType match {
        case _: TimestampMillis | _: TimestampMicros => SchemaType(TimestampType, nullable = false)
        case _ => SchemaType(LongType, nullable = false)
      }

      case ENUM => SchemaType(StringType, nullable = false)

      case NULL => SchemaType(NullType, nullable = true)

      case RECORD =>
        if (existingRecordNames.contains(avroSchema.getFullName)) {
          throw new IncompatibleSchemaException(s"""
                                                   |Found recursive reference in Avro schema, which can not be processed by Spark:
                                                   |${avroSchema.toString(true)}
          """.stripMargin)
        }
        val newRecordNames = existingRecordNames + avroSchema.getFullName
        val fields = avroSchema.getFields.asScala.map { f =>
          val schemaType = toSqlTypeHelper(f.schema(), newRecordNames)
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields), nullable = false)

      case ARRAY =>
        val schemaType = toSqlTypeHelper(avroSchema.getElementType, existingRecordNames)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case MAP =>
        val schemaType = toSqlTypeHelper(avroSchema.getValueType, existingRecordNames)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case UNION =>
        resolveUnionType(avroSchema, existingRecordNames) match {
          case (schema, nullable) =>
            toSqlTypeHelper(schema, existingRecordNames).copy(nullable = nullable)
        }

      case other => throw new IncompatibleSchemaException(s"Unsupported type $other")
    }
  }

  /**
   * Resolves an avro UNION type to an SQL-compatible avro type. Converts complex unions to records
   * if necessary.
   */
  def resolveUnionType(
                        avroSchema: Schema,
                        existingRecordNames: Set[String],
                        nullable: Boolean = false): (Schema, Boolean) = {
    if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
      // In case of a union with null, eliminate it, and make a recursive call
      val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
      if (remainingUnionTypes.size == 1) {
        (remainingUnionTypes.head, true)
      } else {
        resolveUnionType(
          Schema.createUnion(remainingUnionTypes.asJava),
          existingRecordNames,
          nullable = true)
      }
    } else avroSchema.getTypes.asScala.map(_.getType) match {
      case Seq(t1) =>
        (avroSchema.getTypes.get(0), true)
      case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
        (Schema.create(LONG), false)
      case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
        (Schema.create(DOUBLE), false)
      case _ =>
        // Convert complex unions to records where field names are member0, member1, etc.
        // This is consistent with the behavior when converting between Avro and Parquet.
        val record = SchemaBuilder.record(avroSchema.getName).fields()
        avroSchema.getTypes.asScala.zipWithIndex.foreach {
          case (s, i) =>
            // All fields are nullable because only one of them is set at a time
            record.name(s"member$i").`type`(SchemaBuilder.unionOf()
                .`type`(Schema.create(NULL)).and
                .`type`(s).endUnion())
              .withDefault(null)
        }
        (record.endRecord(), false)
    }
  }

  def toAvroType(
                  catalystType: DataType,
                  nullable: Boolean = false,
                  recordName: String = "topLevelRecord",
                  nameSpace: String = "")
  : Schema = {
    val builder = SchemaBuilder.builder()

    val schema = catalystType match {
      case BooleanType => builder.booleanType()
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType => builder.longType()
      case DateType =>
        LogicalTypes.date().addToSchema(builder.intType())
      case TimestampType =>
        LogicalTypes.timestampMicros().addToSchema(builder.longType())

      case FloatType => builder.floatType()
      case DoubleType => builder.doubleType()
      case StringType => builder.stringType()
      case NullType => builder.nullType()
      case d: DecimalType =>
        val avroType = LogicalTypes.decimal(d.precision, d.scale)
        val fixedSize = minBytesForPrecision(d.precision)
        // Need to avoid naming conflict for the fixed fields
        val name = nameSpace match {
          case "" => s"$recordName.fixed"
          case _ => s"$nameSpace.$recordName.fixed"
        }
        avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize))

      case BinaryType => builder.bytesType()
      case ArrayType(et, containsNull) =>
        builder.array()
          .items(toAvroType(et, containsNull, recordName, nameSpace))
      case MapType(StringType, vt, valueContainsNull) =>
        builder.map()
          .values(toAvroType(vt, valueContainsNull, recordName, nameSpace))
      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        val fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields()
        st.foreach { f =>
          val fieldAvroType =
            toAvroType(f.dataType, f.nullable, f.name, childNameSpace)
          fieldsAssembler.name(f.name).`type`(fieldAvroType).noDefault()
        }
        fieldsAssembler.endRecord()

      // This should never happen.
      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }
    if (nullable && catalystType != NullType) {
      Schema.createUnion(schema, nullSchema)
    } else {
      schema
    }
  }
}

class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)
