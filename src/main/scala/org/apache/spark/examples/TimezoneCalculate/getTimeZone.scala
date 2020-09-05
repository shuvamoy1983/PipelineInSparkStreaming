package org.apache.spark.examples.TimezoneCalculate

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

object getTimeZone {

  def convertToIST: String= {
    val date = new Date()
    val dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    dateformat.setTimeZone(TimeZone.getTimeZone("IST"))
    dateformat.format(date)
  }
}
