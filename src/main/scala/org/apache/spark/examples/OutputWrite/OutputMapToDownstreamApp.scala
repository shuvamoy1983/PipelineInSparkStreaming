package org.apache.spark.examples.OutputWrite

import java.nio.file.{Files, Paths}
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.SparkConf
import org.apache.spark.examples.SourceConfiguration.Configurations
import org.apache.spark.examples.Utils.readFileFromResource
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.examples.sparkSession.ConnectSession.{getMysql, getSparkSession}
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions.col
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

import scala.collection.mutable.ListBuffer

object OutputMapToDownstreamApp {

  val spark: SparkSession = getSparkSession
  val mysql:(String,String,String) =getMysql

  def writeKafkaToMultipleApp(topicName : ListBuffer[String], schemaName: ListBuffer[String], KafkaIP:String,config: Configurations) = {


    val topics=topicName.mkString(",")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$KafkaIP:9094")
      .option("subscribe", s"$topics")
      .option("startingOffsets", "latest") //latest
      .load()

    df.select(col("topic"), col("value")).writeStream
      .foreachBatch {
        (df: Dataset[Row], _: Long) =>
          //df.persist()
          val tpc=df.select("topic").distinct().collect().map(_.getString(0)).mkString(" ")

          if (!tpc.isEmpty) {
            val getPartialTopicName = tpc.split("-")(1)
            println(getPartialTopicName)
            if(schemaName.contains(s"$getPartialTopicName.avsc")) {

              val avroFormatSchema = new String(
                Files.readAllBytes(Paths.get(readFileFromResource.readFromResource(s"/schema/$getPartialTopicName.avsc").getAbsolutePath)))

              val topicDF = df.filter(col("topic") === tpc).select(from_avro(col("value"), avroFormatSchema).as("data"))
                .select("data.*")


              if (!(config.outputOptions.apply("DB") ==null)) {
                val prop = new java.util.Properties
                prop.setProperty("driver", "com.mysql.jdbc.Driver")
                prop.setProperty("user", mysql._1)
                prop.setProperty("password", mysql._2)
                val url = s"jdbc:mysql://${mysql._3}:3306/mydb"

                topicDF.write.mode("append").jdbc(url, getPartialTopicName, prop)
              }

              topicDF.show()
              // Write to Cassandra
              if (!(config.outputOptions.apply("NoSqlDb") ==null)) {
                topicDF.write
                  .format("org.apache.spark.sql.cassandra")
                  .options(Map("table" -> getPartialTopicName, "keyspace" -> "mydb"))
                  .mode("APPEND")
                  .save()
              }

            }
            // df.unpersist()
          }
      }.start().awaitTermination()
    //val out = df.writeStream.format("console")
    //.option("truncate", "false").start().awaitTermination()



  }

}
