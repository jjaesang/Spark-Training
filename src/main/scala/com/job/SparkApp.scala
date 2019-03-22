package com.job

import org.apache.spark.SparkConf

/**
  * Created by jaesang on 2019-03-15.
  * reference by : https://github.com/apache/incubator-s2graph/blob/master/spark/src/main/scala/org/apache/s2graph/spark/spark/SparkApp.scala*
  */
trait SparkApp {

  protected def args: Array[String] = _args

  private var _args: Array[String] = _

  def run()

  def getArgs(index: Int) = args(index)

  def main(args: Array[String]) {
    _args = args
    run()
  }

  def validateArgument(argNames: String*): Unit = {
    if (args == null || args.length < argNames.length) {
      System.err.println(s"Usage: ${getClass.getName} " + argNames.map(s => s"<$s>").mkString(" "))
      System.exit(1)
    }
  }

  def sparkConf(jobName: String): SparkConf = {
    val conf = new SparkConf()
    conf.setAppName(jobName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.streaming.unpersist", "true")
    conf
  }

}
