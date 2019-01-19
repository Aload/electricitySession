package utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

object DateUtils {
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
    * 获取当前的时间
    *
    * @return
    */
  def getTodayDate(): String = {
    DATE_FORMAT.format(new Date())
  }

  /**
    * 格式化日期（yyyy-MM-dd）
    *
    * @param date Date对象
    * @return 格式化后的日期
    */
  def formatDate(date: Date): String = DATE_FORMAT.format(date)

  /**
    * 解析时间字符串
    *
    * @param time 时间字符串
    * @return
    */
  def parseTime(time: String): Date = {
    try
      TIME_FORMAT.parse(time)
    catch {
      case e: ParseException =>
        e.printStackTrace()
    }
    null
  }

  /**
    * 获取年月日和小时
    *
    * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
    * @return 结果
    */
  def getDateHour(datetime: String): String = {
    val date = datetime.split(" ")(0)
    val hourMinuteSecond = datetime.split(" ")(1)
    val hour = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }
}
