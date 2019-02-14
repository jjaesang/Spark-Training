package com.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery}

/**
  * Created by jaesang on 2019-02-13.
  */
object StreamingEventCount {

  val sparkConf = new SparkConf()
    .setAppName("Streaming Exercise : Event count Processing ")
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

    val activityCounts = streaming.groupBy("gt").count()

    import scala.concurrent.duration._
    val activityQuery = activityCounts
      .writeStream
      .trigger(ProcessingTime(1.seconds)) //dafault 0L
      .queryName("activity_counts")
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()

    activityQuery.awaitTermination()

    /*
          batch 2
          +----------+-----+
          |        gt|count|
          +----------+-----+
          |  stairsup|10452|
          |       sit|12309|
          |     stand|11384|
          |      walk|13256|
          |      bike|10796|
          |stairsdown| 9365|
          |      null|10449|
          +----------+-----+
          ... __ ..

          batch 5
          +----------+------+
          |        gt| count|
          +----------+------+
          |  stairsup|114975|
          |       sit|135392|
          |     stand|125234|
          |      walk|145816|
          |      bike|118773|
          |stairsdown|103010|
          |      null|114931|
          +----------+------+
      */

    /**
      * .format("memory") 일 경우, 스트리밍 작업이 실행된 SparkSession을 통해
      * 결과 질의 가능
      */
    val activeStreamingQueryList: Array[StreamingQuery] = activityQuery.sparkSession.streams.active
    activeStreamingQueryList.foreach(streamingQuery => {
      val name = streamingQuery.name
      val id = streamingQuery.id            // static query id
      val runId = streamingQuery.runId      // dynamic query id , if start/restart query will be changed name
      val isActive = streamingQuery.isActive
      val status = streamingQuery.status
      val jsonPretteyMsg = status.message.toString
      println(s"$name Query (id:$runId) is $isActive status, and message : $jsonPretteyMsg ")
    })

    activityQuery.sparkSession.sql("select * from activity_counts")
      .show()

  }

}
