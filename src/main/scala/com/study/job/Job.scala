package com.study.job

import org.apache.spark.sql.SparkSession

/**
  * Created by jaesang on 2019-03-14.
  */
class Job(spark: SparkSession, jobDesc: JobDescription) extends Serializable with Logger {

  def run() = {

  }

}
