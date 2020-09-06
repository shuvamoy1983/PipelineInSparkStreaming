package org.apache.spark.examples.OutputWrite
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.examples.SourceConfiguration.Configurations
import org.apache.spark.examples.Utils.readFileFromResource
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.examples.sparkSession.ConnectSession.getSparkSession
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

object OutputMapToDownstreamApp {

  val spark: SparkSession = getSparkSession

  //conf.setMaster("local[*]")

  def writeKafkaToMultipleApp(schemaName: String, topicName: String,config: Configurations,lenOfSchema: Int) = {

    while (true) {
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "35.245.237.47:9094")
        .option("subscribe", topicName)
        .option("startingOffsets", "latest") //latest
        .load()

      val avroFormatSchema = new String(
        Files.readAllBytes(Paths.get(readFileFromResource.readFromResource(s"/schema/$schemaName").getAbsolutePath)))

      print(avroFormatSchema)

      val topicDF = df.select(from_avro(col("value"), avroFormatSchema).as("employee"))
        .select("employee.*")



      /* val out = topicDF.writeStream.format("console")
        .option("truncate", "false").start() */

      val InputtableDataSet = topicName.split("-")(1)
      val getTableNm = s"${config.outputOptions.apply("targetTable")}"
      val tablename = getTableNm.split(",")(lenOfSchema)

      if (InputtableDataSet == tablename) {
        println("I am writing to Mysql", tablename)
        topicDF.writeStream.outputMode(OutputMode.Update()).option("truncate", false)
          .foreachBatch {
            (df: Dataset[Row], _: Long) =>
              df.show(false)
              val cnt = df.count()
              if (cnt > 0) {
                val prop = new java.util.Properties
                prop.setProperty("driver", "com.mysql.jdbc.Driver")
                prop.setProperty("user", "root")
                prop.setProperty("password", "password")
                val url = s"jdbc:mysql://35.230.188.90:3306/mydb"
                val table = tablename
                df.write.mode("append").jdbc(url, table, prop)
              }

          }.start()

        Thread.sleep(2000L)
      }

    }


  }
}