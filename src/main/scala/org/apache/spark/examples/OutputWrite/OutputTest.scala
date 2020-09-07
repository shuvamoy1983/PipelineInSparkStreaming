package org.apache.spark.examples.OutputWrite

import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.examples.SourceConfiguration.Configurations
import org.apache.spark.examples.Utils.readFileFromResource
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.examples.sparkSession.ConnectSession.getSparkSession
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions.col
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

import scala.collection.mutable.ListBuffer

object OutputTest {

  val spark: SparkSession = getSparkSession


  def writeKafkaToMultipleApp(topicName : ListBuffer[String], schemaName: ListBuffer[String], config: Configurations) = {
    val topics=topicName.mkString(",")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ":9094")
      .option("subscribe", s"$topics")
      .option("startingOffsets", "latest") //latest
      .load()

    df.select(col("topic"), col("value")).writeStream
      .foreachBatch {
        (df: Dataset[Row], _: Long) =>
          //df.persist()
          val tpc=df.select("topic").distinct().collect().map(_.getString(0)).mkString(" ")
          println(tpc)
          if (!tpc.isEmpty) {
            val getPartialTopicName = tpc.split("-")(1)
            println(getPartialTopicName)
            if(schemaName.contains(s"$getPartialTopicName.avsc")) {

              val avroFormatSchema = new String(
                Files.readAllBytes(Paths.get(readFileFromResource.readFromResource(s"/schema/$getPartialTopicName.avsc").getAbsolutePath)))

              val topicDF = df.filter(col("topic") === tpc).select(from_avro(col("value"), avroFormatSchema).as("data"))
                .select("data.*")

              val prop = new java.util.Properties
              prop.setProperty("driver", "com.mysql.jdbc.Driver")
              prop.setProperty("user", "root")
              prop.setProperty("password", "password")
              val url = s"jdbc:mysql://:3306/mydb"
              val table = getPartialTopicName
              topicDF.write.mode("append").jdbc(url, table, prop)

              topicDF.show()
              // Write to Cassandra
              topicDF.write
                .format("org.apache.spark.sql.cassandra")
                .options(Map( "table" -> table, "keyspace" -> "mydb"))
                .mode("APPEND")
                .save()


            }



            // df.unpersist()
          }
      }.start().awaitTermination()
     //val out = df.writeStream.format("console")
      //.option("truncate", "false").start().awaitTermination()



  }

}
