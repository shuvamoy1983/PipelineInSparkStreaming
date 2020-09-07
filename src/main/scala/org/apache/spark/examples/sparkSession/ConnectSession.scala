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
  private var mysql: (String,String,String)=("","","")

  val resource="/Pool.xml"
  val sparktmpFile = readFileFromResource.readFromxmlResource(resource)
  private var myJDBC: Option[String] = None

  def init(config: Configurations,MySqlHost: String,MySqlUserName: String
           ,MySqlPassword: String,cassandraHost: String,cassandraUserName: String,cassandraPassword: String) {
    getLog.info(s"Get Spark session for ${config.appName} ")
    spark = Some(createSparkSession(config.appName,cassandraHost,cassandraUserName,cassandraPassword))
    mysql=CreateMySqlJDBC(MySqlHost,MySqlUserName,MySqlPassword)

  }

  private def createSparkSession(appName: String,cassandraHost:String,cassandraUserName:String,cassandraPassword:String): SparkSession = {
    val conf = new SparkConf()
      .setAppName("Connecting to Spark")
      .set("spark.scheduler.pool","FAIR")
      .set("spark.scheduler.allocation.file",s"$sparktmpFile")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.dynamicAllocation.testing","true")
      .set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite","true")
      .set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite","true")
      .set("spark.cassandra.connection.host",cassandraHost)
      .set("spark.cassandra.connection.port","9042")
      .set("spark.cassandra.auth.username",cassandraUserName)
      .set("spark.cassandra.auth.password",cassandraPassword)

    val session = SparkSession.builder().appName(appName).config(conf).master("local").getOrCreate()
    session.sparkContext.setLocalProperty("spark.scheduler.pool", "fair_pool")
    session
  }


  def CreateMySqlJDBC(MySqlHost:String,MySqlUserName:String,MySqlPassword:String): (String,String,String)= {

  //  val conLoad = ConfigFactory.load()
   // val ConParameters=conLoad.getConfig(env)
    val UserName=MySqlUserName
    val password=MySqlPassword
    val Host=MySqlHost

    return (UserName,password,Host)

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

  def getMysql: (String,String,String) = {
    return (mysql._1,mysql._2,mysql._3)
  }

}
