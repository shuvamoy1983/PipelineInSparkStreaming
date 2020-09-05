package org.apache.spark.examples

import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.examples.schmaJson.JsonStructures._
import org.apache.spark.examples.FileParsing.fileParse.parseYamlFile
import org.apache.spark.examples.LogInfoDetails.getLogInfo.getLog
import org.apache.spark.examples.OutputWrite.OutputMapToDownstreamApp.spark
import org.apache.spark.examples.SourceConfiguration.Configurations
import org.apache.spark.examples.TimezoneCalculate.getTimeZone.convertToIST
import org.apache.spark.examples.Utils.readFileFromResource
import org.apache.spark.examples.allInputFiles.ListOfInputFile
import org.apache.spark.examples.sparkSession.ConnectSession
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object ExecuteMain {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[*]")

    val spark = SparkSession.builder().appName("WriteToES").config(conf).getOrCreate()
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "35.245.237.47:9094")
      .option("subscribe", "topic-emp")
      .option("startingOffsets", "latest") //latest
      .load()

    val jsonFormatSchema = new String(
      Files.readAllBytes(Paths.get(readFileFromResource.readFromResource("/schema/emp.avsc").getAbsolutePath)))


    val personDF = df.select(from_avro(col("value"), jsonFormatSchema).as("employee"))
      .select("employee.*")

    personDF.writeStream
      .foreachBatch {
        (df: Dataset[Row], _: Long) =>
          df.show(false)
      }.start().awaitTermination()


  }
}
