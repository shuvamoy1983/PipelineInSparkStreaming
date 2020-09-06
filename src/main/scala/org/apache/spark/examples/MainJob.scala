package org.apache.spark.examples

import org.apache.spark.examples.LogInfoDetails.getLogInfo._
import org.apache.spark.examples.TimezoneCalculate.getTimeZone._
import org.apache.spark.examples.allInputFiles.ListOfInputFile
import org.apache.spark.examples.FileParsing.fileParse._
import org.apache.spark.examples.SourceConfiguration.Configurations
import org.apache.spark.examples.sparkSession.ConnectSession
import org.apache.spark.examples.sparkSession.ConnectSession._
import org.apache.spark.examples.OutputWrite.OutputMapToDownstreamApp._
import org.apache.spark.examples.Utils.readFileFromResource
import scala.collection.mutable.ListBuffer
import org.apache.spark.examples.OutputWrite.OutputTest

object MainJob {
  def main(args: Array[String]): Unit = {
    getLog.info("Gcp Data migration Job Started")
    val startTime=convertToIST
    val ListOfFiles=ListOfInputFile.ListOfFilesToRead

    // Execution Started for each input File
    ListOfFiles.foreach({ resourcePath =>
      val tmpLoc = readFileFromResource.readFromInputResource(resourcePath)
      val config: Configurations = parseYamlFile(s"$tmpLoc")

      ConnectSession.init(config)
      val mysqlConn=ConnectSession.CreateMySqlJDBC(config.Environment)
      println(mysqlConn._1,mysqlConn._2)

      val mytopicList =new ListBuffer[String]()
      val mySchemaList =new ListBuffer[String]()


        var lenOfSchema=config.KafkaSchemaDataSetName.length -1
        //writeKafkaToMultipleApp()
        config.KafkaSchemaDataSetName.foreach { schema =>
          val KafkaTopic = config.KafkaTopic(lenOfSchema)
          val schemainfo = config.KafkaSchemaDataSetName(lenOfSchema)
          mytopicList +=KafkaTopic
          mySchemaList +=schemainfo


         // writeKafkaToMultipleApp(schemainfo, KafkaTopic,config,lenOfSchema)
          Thread.sleep(4000L)
          lenOfSchema = lenOfSchema - 1
        }

      OutputTest.writeKafkaToMultipleApp(mytopicList,mySchemaList,config)

    })

  }
}
