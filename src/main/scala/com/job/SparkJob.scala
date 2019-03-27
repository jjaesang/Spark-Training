package com.job

/**
  * Created by jaesang on 2019-03-27.
  */
trait SparkJob[T] {

  /**
    * treat all I/O operations or Spark Action
    * so need to implement Job class that do Spark transformation
    * @param args
    */
  def run(args: Array[String])

  /**
    * parse Argument using scopt
    * @param args
    * @return
    */
  def parseArgument(args: Array[String]) : T


}
