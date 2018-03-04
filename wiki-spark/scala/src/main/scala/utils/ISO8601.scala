package utils

import java.text.ParseException

import java.text.SimpleDateFormat

import java.util.Calendar

import java.util.Date

import java.util.TimeZone

//remove if not needed
import scala.collection.JavaConversions._

object ISO8601 {

  /**
   * Transform timestamp(ms) to Calendar
   */
  def timestampToString(timestamp: Long): String = {
    val calendar: Calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp)
    val date: Date = calendar.getTime
    val sdf: SimpleDateFormat = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss'Z'")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
    val formatted: String = sdf.format(date)
    formatted
  }

  /**
   * Transform ISO 8601 string to timestamp.
   */
  def toTimeMS(iso8601string: String): Long = {
    var s: String = iso8601string.replace("Z", "+00:00")
    s = s.substring(0, 22) + s.substring(23)
    val date: Date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(s)
    date.getTime
  }

}
