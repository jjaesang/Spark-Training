package com.job.task.source

import com.job.task.TaskConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by jaesang on 2019-03-22.
  */
class FileSource(conf: TaskConf) extends Source(conf) {
  override def toDF(spark: SparkSession): DataFrame = {
    val paths = conf.options("paths").split(",")
    val format = conf.options.getOrElse("format", "parquet")

    spark.read
      .format(format)
      .load(paths: _*)
  }

  override def mandatoryOptions: Set[String] = {
    Set("format", "paths")
  }
}
