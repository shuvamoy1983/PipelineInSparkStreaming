package org.apache.spark.examples

import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.examples.SourceConfiguration.Configurations
import org.apache.spark.examples.Utils.readFileFromResource
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.examples.sparkSession.ConnectSession.getSparkSession
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions.{col, from_json, struct}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructType}

import scala.collection.mutable.ListBuffer


object localtest {

  val spark: SparkSession = getSparkSession

  def writeKafkaToMultipleApp1(topicName: ListBuffer[String], schemaName: ListBuffer[String], config: Configurations) = {
    val topics = topicName.mkString(",")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "bankserver1.mydb.emp")
      .option("multiLine", true)
      .option("startingOffsets", "latest") //latest
      .load()

    df.printSchema()
    val StringDF = df.selectExpr("CAST(value AS STRING)")

    val structureSchema = new StructType()
      .add("payload",new StructType()
        .add("after",new StructType()
        .add("id",IntegerType)
        .add("name",StringType)
        .add("salary",IntegerType)))

    val DF = StringDF.select(from_json(col("value"), structureSchema).as("data"))
     // .select("data.*")

    DF.select("data.*").writeStream
      .foreachBatch {
        (df: Dataset[Row], _: Long) =>
          df.select("payload.after.*").show()
         // df.show()
      }
           .start()
           .awaitTermination()


  }
}
