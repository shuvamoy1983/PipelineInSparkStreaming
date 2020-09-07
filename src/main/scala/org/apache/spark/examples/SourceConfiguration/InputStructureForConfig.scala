package org.apache.spark.examples.SourceConfiguration

trait InputStructure {

    def explain: Boolean

    def logLevels: String

    def FileConversion: String

    def outputOptions: Map[String,String]

    def appName: String

    def Environment : String

    def SourceType : String

    def KafkaTopic: List[String]

    def KafkaSchemaDataSetName: List[String]


  }
