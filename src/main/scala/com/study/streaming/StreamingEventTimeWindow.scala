package com.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery}

/**
  * Created by jaesang on 2019-02-13.
  */
object StreamingEventTimeWindow {

  val sparkConf = new SparkConf()
    .setAppName("Streaming Exercise : Time Window Processing ")
    .setMaster("local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val input = "data/activity-data"

    val data = spark.read.json(input)

    val streaming = spark.readStream
      .schema(data.schema)
      .option("maxFilesPerTrigger", 1)
      .json(input)


    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    import org.apache.spark.sql.functions.{window, col}
    /**
      * 텀블링 윈도우 ( tumbling window )
      * window size == advanced interval
      * 간단하게, 그냥 윈도우 마다 처리 / 겹치는 일 없음
      *
      * windows size = 타임 윈도우 간격 10분
      * advanced interval = 얼마나 자주 윈도우를 이동시킬 것인가
      *
      */
    withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()
      .writeStream
      .queryName("events_per_window")
      .format("console")
      .outputMode(OutputMode.Complete)
      .start()

    /**
      * 슬라이딩 윈도우
      * 5분마다 실행하는 10분짜리 윈도우
      */
    withEventTime.groupBy(window(col("event_time"), "10 minutes", "5 minutes")).count()
      .writeStream
      .queryName("events_per_window")
      .format("console")
      .outputMode(OutputMode.Complete)
      .start()
  }

}
