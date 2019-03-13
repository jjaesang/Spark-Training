package com.study.sql

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by jaesang on 2019-03-13.
  *
  * DB에 저장된 데이터값이 String 일 때, Spark SQL 내에서 날짜 기반 필터링을 수행해야할 경우,
  * 모든 날짜 데이터를 Unix Timestamp로 변경 후, 필터링 할 수 있다.
  *
  * .. 처음부터 Date 형식으로 넣는게 좋겠지..?
  *
  */
object DateFilterBetweenTimestamp {

  val DATE_STR_FORMAT_YYMMDDHH = "yyMMddHH"

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Spark SQL Exercise : Filter data using String Date type ")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val startTime = "19031312"
    val endTime = "19031313"

    val startTimeStamp = convertStringtoTimestamp(startTime)
    val endTimeStamp = convertStringtoTimestamp(endTime)
    /**
      *  startTimeStamp: java.sql.Timestamp = 2019-03-13 12:00:00.0
      *  endTimeStamp: java.sql.Timestamp = 2019-03-13 13:00:00.0
      */


    import spark.implicits._
    import org.apache.spark.sql.functions._

    val dateDF = Seq("19031310", "19031312", "19031313", "19031315")
      .toDF("time")

    /**
      * String으로 입력된 날짜값을 unix_timstamp로 변경 후
      * between 함수를 통해, 특정 날짜 사이의 데이터만 필터링 할 수 있다.
      *
      * +--------+
      * |time    |
      * +--------+
      * |19031312|
      * |19031313|
      * +--------+
      */

    dateDF.filter {
      unix_timestamp($"time", DATE_STR_FORMAT_YYMMDDHH)
        .cast("timestamp")
        .between(startTimeStamp, endTimeStamp)
    }.show(false)


  }

  def convertStringtoTimestamp(dateStr: String): Timestamp = {
    new Timestamp(
      new SimpleDateFormat(DATE_STR_FORMAT_YYMMDDHH)
        .parse(dateStr)
        .getTime
    )
  }

}
