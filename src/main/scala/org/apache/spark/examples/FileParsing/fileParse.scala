package org.apache.spark.examples.FileParsing

import java.io.{File, FileNotFoundException, FileReader}

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.examples.SourceConfiguration.Configurations
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.commons.io.{FileUtils, FilenameUtils}

object fileParse {

  def ExtensionChk(file: String) : Option[ObjectMapper]={
    val ext= FilenameUtils.getExtension(file)
    ext match {
      case "yaml" | "yml" | _ => Option(new ObjectMapper(new YAMLFactory()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false))
    }
  }

  def getListOfFiles(file: String) : List[File]={
    val d= new File(file)
    if (d.isDirectory){
      d.listFiles.filter(_.isFile).toList
    } else if (d.isFile) {
      List(d)
    }
    else {
      throw new FileNotFoundException(s"No files to run ${file}")
    }
  }

  def checkBlankOrNull(input : Option[String]) : Boolean={
    input.isEmpty ||
    input.filter(_.trim.length > 0).isEmpty
  }

  def parseYamlFile(file: String): Configurations= {
    ExtensionChk(file) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new FileReader(file),classOf[Configurations])
      }
      case None => throw new FileNotFoundException("File not found")
    }
  }

  def getContentFromFile(file: File): String = {
    scala.io.Source.fromFile(file).mkString
  }

}
