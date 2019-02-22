  package com.study.streaming

  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.streaming.OutputMode

  /**
    * Created by jaesang on 2019-02-15.
    */
  object StreamingOutputMode {

    val sparkConf = new SparkConf()
      .setAppName("Streaming Exercise : Output Mode Result Compare ")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    def main(args: Array[String]): Unit = {

      val socketStreamDf = spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()

      val socketDs = socketStreamDf.as[String]
      val wordCountDs = socketDs.flatMap(_.split(" "))
        .groupBy("value")
        .count()

      val query =
        wordCountDs.writeStream
          .format("console")
          .outputMode(OutputMode.Complete())

      query.start().awaitTermination()
    }
  }
