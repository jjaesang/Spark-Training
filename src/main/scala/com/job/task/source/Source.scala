package com.job.task.source

import com.job.task.{Task, TaskConf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by jaesang on 2019-03-22.
  */
abstract class Source(override val conf: TaskConf) extends Task {

  def toDF(spark: SparkSession): DataFrame

}
