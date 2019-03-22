package com.job.task.source

import com.job.task.TaskConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by jaesang on 2019-03-22.
  */
class JdbcSource(conf: TaskConf) extends Source(conf) {
  override def toDF(spark: SparkSession): DataFrame = {
    spark.read
      .format("jdbc")
      .options(conf.options)
      .load()
  }

  override def mandatoryOptions: Set[String] = {
    Set("url", "dbtable")
  }
}
