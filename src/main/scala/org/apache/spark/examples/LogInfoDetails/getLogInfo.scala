package org.apache.spark.examples.LogInfoDetails

import org.apache.log4j.{Level, Logger}


object getLogInfo {
  def getLog: org.apache.log4j.Logger= {
    val LOG: Logger = Logger.getLogger("Application")
    LOG.setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.ERROR)
    LOG
  }
}
