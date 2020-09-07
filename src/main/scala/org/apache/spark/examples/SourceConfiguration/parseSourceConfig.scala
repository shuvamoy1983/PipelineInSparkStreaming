package org.apache.spark.examples.SourceConfiguration

import com.fasterxml.jackson.annotation.JsonProperty

class Configurations(
                     @JsonProperty("Environment")  _Environment: String,
                     @JsonProperty("SourceType")  _SourceType: String,
                     @JsonProperty("logLevels") _logLevels: String,
                     @JsonProperty("outputOptions") _outputOptions: Map[String,String],
                     @JsonProperty("appName") _appName: String,
                     @JsonProperty("FileConversion") _FileConversion: String,
                     @JsonProperty("KafkaTopic") _KafkaTopic: List[String],
                     @JsonProperty("KafkaSchemaDataSetName") _KafkaSchemaDataSetName: List[String],
                     @JsonProperty("explain") _explain: Boolean) extends InputStructure {

  require(Option(_SourceType).isDefined,"Need to provide the details for environment")

  val Environment: String = Option(_Environment).getOrElse("Development")
  val SourceType: String = Option(_SourceType).getOrElse("SourceType")
  val explain: Boolean= _explain
  val logLevels: String= Option(_logLevels).getOrElse("WARN")
  val FileConversion: String= Option(_FileConversion).getOrElse("")
  val outputOptions:  Map[String,String]= Option(_outputOptions).getOrElse(Map())
  val appName: String=Option(_appName).getOrElse("AutoGDS")
  val KafkaTopic: List[String]=Option(_KafkaTopic).getOrElse(List())
  val KafkaSchemaDataSetName: List[String]=Option(_KafkaSchemaDataSetName).getOrElse(List())

}