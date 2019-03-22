package com.job.task.source

import com.job.task.TaskConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by jaesang on 2019-03-22.
  */
class KafkaSource(conf: TaskConf) extends Source(conf) {

  override def toDF(spark: SparkSession): DataFrame = {
      spark.readStream
        .format("kafka")
        .options(conf.options)
        .load()
  }

  override def mandatoryOptions: Set[String] = {
    Set("kafka.bootstrap.servers", "subscribe")
  }

}
