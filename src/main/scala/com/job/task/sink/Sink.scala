package com.job.task.sink

import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

/**
  * Created by jaesang on 2019-03-22.
  */
trait Sink {

  val FORMAT: String

  def write(dataframe: DataFrame): Unit = {
    if (dataframe.isStreaming)
      writeStream(dataframe.writeStream)
    else
      writeBatch(dataframe.write)
  }

  protected def writeStream(writer: DataStreamWriter[Row]): Unit = {

  }

  protected def writeBatch(writer: DataFrameWriter[Row]): Unit = {


  }
}