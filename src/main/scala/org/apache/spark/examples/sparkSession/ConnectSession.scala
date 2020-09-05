package org.apache.spark.examples.sparkSession

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import org.apache.spark.examples.SourceConfiguration.Configurations
import org.apache.spark.examples.LogInfoDetails.getLogInfo._
import org.apache.spark.examples.Utils.readFileFromResource

object ConnectSession {

  private var configuration: Option[Configurations] = None
  private var spark: Option[SparkSession] = None
  val resource="/Pool.xml"
  val sparktmpFile = readFileFromResource.readFromxmlResource(resource)
  private var myJDBC: Option[String] = None

  def init(config: Configurations) {
    getLog.info(s"Get Spark session for ${config.appName} ")
    spark = Some(createSparkSession(config.appName))

  }

  private def createSparkSession(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setAppName("Connecting to Spark")
      .set("spark.scheduler.pool","FAIR")
      .set("spark.scheduler.allocation.file",s"$sparktmpFile")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.dynamicAllocation.testing","true")
    val session = SparkSession.builder().appName(appName).config(conf).master("local").getOrCreate()
    session.sparkContext.setLocalProperty("spark.scheduler.pool", "fair_pool")
    session
  }


  def CreateMySqlJDBC(Environment: String): (String,String,String)= {

    val server=Environment
    val env = if (System.getenv("DEFAULTENV") == null)
      s"$server"
    else System.getenv("DEFAULTENV")

    val conLoad = ConfigFactory.load()
    val ConParameters=conLoad.getConfig(env)
    val UserName=ConParameters.getString("app.MySqlUserName")
    val password= ConParameters.getString("app.MySqlUserPwd")
    val port=ConParameters.getString("app.MySqlPort")

    return (UserName,password,port)

  }


  def getConfiguration: Configurations = {
    if (configuration.isDefined) {
      configuration.get
    }
    else {
      throw new Exception(s"Session Configuration Must Be Set")
    }
  }



  def getSparkSession: SparkSession = {
    if (spark.isDefined) {
      spark.get
    }
    else {
      throw new Exception(s"Session Configuration Must Be Set")
    }
  }


}
