package com.job.task.process

import com.job.task.TaskConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by jaesang on 2019-03-22.
  */
class SqlProcess(conf: TaskConf) extends Process(conf) {
  override def execute(spark: SparkSession, inputMap: Map[String, DataFrame]): DataFrame = {

    inputMap.foreach {
      case (name, df) => {
        df.createOrReplaceTempView(name)
      }
    }
    val sqlStatement = conf.options("sql")
    spark.sql(sqlStatement)
  }

  override def mandatoryOptions: Set[String] = {
    Set("sql")
  }
}
