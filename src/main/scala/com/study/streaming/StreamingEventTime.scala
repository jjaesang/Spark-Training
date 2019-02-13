package com.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by jaesang on 2019-02-13.
  */
object StreamingEventTime {

  val sparkConf = new SparkConf()
    .setAppName("Streaming Exercise : EventTime Processing ")
    .setMaster("local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val input = "data/activity-data"

    val data = spark.read.json(input)
    val streaming = spark.readStream
      .schema(data.schema)
      .option("maxFilesPerTrigger", 10)
      .json(input)

    streaming.printSchema()

    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    import org.apache.spark.sql.functions._

    withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode(OutputMode.Complete())
      .start()

  }

}
