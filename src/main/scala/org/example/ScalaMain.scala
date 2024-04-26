package org.example

import example.avro.User
import org.apache.spark.sql.SparkSession

object ScalaMain {
  def main(args: Array[String]): Unit = {
    println("hello scala")

    val spark = SparkSession.builder.master("local[*]").getOrCreate
    val ds = spark.range(1,10).map(l=>new User("a",l.toInt,"red"))(AvroEncoder.of(classOf[User]))
    ds.printSchema()

    // run in databricks
//    ds.writeTo("user").createOrReplace()

    // run in local
    ds.write.mode("overwrite").option("compression","zstd").parquet("parquet")

//    val s = StaticInvoke(
//      classOf[UTF8String],
//      StringType,
//      "fromString",
//      Invoke(Literal("hello"), "toString", ObjectType(classOf[java.lang.String])) :: Nil)
//    println(s)
  }
}
