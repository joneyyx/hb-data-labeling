package com.cummins.dataLabeling.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

object DateUtil {
  var FORMAT_TIME = "yyyy-MM-dd HH:mm:ss"
  var FORMAT_DATE = "yyyy-MM-dd"


  @throws[Exception]
  def getBeijingDateBeforeWeek(beforeWeek: Int): String = {
    var mondayOfLastWeek = getBeijingMondayOfLastWeek("yyyy-MM-dd")
    mondayOfLastWeek = add(mondayOfLastWeek, Calendar.DATE, (beforeWeek - 1) * (-7))
    formatTimeToDate(mondayOfLastWeek)
  }

  @throws[Exception]
  def getBeijingMondayOfLastWeek(format: String): Date = {
    val date = getCurrentBeijingTime
    val c = Calendar.getInstance
    c.setTime(date)
    var dayOfWeek = c.get(Calendar.DAY_OF_WEEK) - 1
    if (dayOfWeek == 0) dayOfWeek = 7
    c.add(Calendar.DATE, -dayOfWeek - 6)
    c.getTime
  }

  def add(date: Date, field: Int, amount: Int) = {
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(field, amount)
    c.getTime
  }

  @throws[Exception]
  def getBeijingYesterday: String = {
    var date = getCurrentBeijingTime
    date = add(date, Calendar.DATE, -1)
    formatTimeToDate(date)
  }

  @throws[Exception]
  def getCurrentBeijingTime = {
    val sdf = new SimpleDateFormat(FORMAT_TIME)
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val currBeiJingDate = sdf.format(new Date)
    val format = new SimpleDateFormat(FORMAT_TIME)
    format.parse(currBeiJingDate)
  }

  def formatTimeToDate(time: Date): String = {
    val sdf = new SimpleDateFormat(FORMAT_DATE)
    sdf.format(time)
  }

  @throws[Exception]
  def getCurrentBeijingDate: String = {
    val date = getCurrentBeijingTime
    formatTimeToDate(date)
  }

  @throws[Exception]
  def getBeijing10DaysBefore: String = {
    var date = getCurrentBeijingTime
    date = add(date, Calendar.DATE, -10)
    formatTimeToDate(date)
  }

}
