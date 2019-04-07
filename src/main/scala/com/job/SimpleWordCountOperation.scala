package com.job

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class SimpleWordCountOperation(spark : SparkSession) extends Serializable with Logger {

  def wordCount(input: RDD[String]): RDD[(String, Int)] = {

    logger.info("simple wordcount exercise for sparkApp Template")

    input.flatMap(_.split(" "))
      .map(row => (row, 1))
      .reduceByKey(_ + _)

  }

}
