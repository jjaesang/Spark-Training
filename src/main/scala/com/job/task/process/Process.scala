package com.job.task.process

import com.job.task.{Task, TaskConf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by jaesang on 2019-03-22.
  */
abstract class Process(override val conf: TaskConf) extends Task {
  def execute(spark: SparkSession, inputMap: Map[String, DataFrame]): DataFrame

}
