
package com.job

import org.apache.spark.sql.SparkSession

/**
  * Created by jaesang on 2019-03-14.
  */
class Job(spark: SparkSession) extends Serializable with SparkApp with Logger {

  override def run(): Unit = {

    //do something
  }

}

