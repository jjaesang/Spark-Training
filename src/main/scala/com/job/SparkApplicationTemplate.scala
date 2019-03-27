package com.job

import org.apache.spark.sql.SparkSession

/**
  * Created by jaesang on 2019-03-27.
  */

case class SparkApplicationConfiguration(
                                          input: String = "",
                                          output: String = "",
                                          option: String = ""
                                        )

class SparkApplicationTemplate(spark : SparkSession) extends SparkJob[SparkApplicationConfiguration] {
  /**
    * treat all I/O operations or Spark Action
    * so need to implement Job class that do Spark transformation
    *
    * @param args
    */
  override def run(args: Array[String]): Unit = {

    val options = parseArgument(args)
    /**
      * Using Option , get user defined argument
      */
    val input = options.input
    val output = options.output

    val data = spark.read.format("parquet").load(input)

    /**
      * do Something like transform  using Job describe Class
      *
      * ex) val sparkJobOperation = new SparkJobOperation(spark)
      *
      * val result = sparkJobOperation.wordcount(data)
      */

    data.write.parquet(output)

  }

  /**
    * parse Argument using scopt
    *
    * @param args
    * @return
    */
  override def parseArgument(args: Array[String]): SparkApplicationConfiguration = {

    val parser = new scopt.OptionParser[SparkApplicationConfiguration]("run") {
      head(s"arguemnt parse using scopt for ${this.getClass.getName} ", "1.x")

      opt[String]("input").abbr("i").required().valueName("<input-path>").action {
        (arg, config) => config.copy(input = arg)
      } text "input path"
      opt[String]("output").abbr("o").required().valueName("<input-path>").action {
        (arg, config) => config.copy(output = arg)
      } text "output path "
      opt[String]("option").abbr("op").valueName("<option-for-u-need>").action {
        (arg, config) => config.copy(option = arg)
      } text "option for u "
    }

    val options = parser.parse(args, SparkApplicationConfiguration()) match {
      case Some(option) =>
        option
      case None =>
        parser.showUsage
        throw new IllegalArgumentException(s"failed to parse options... (${args.mkString(",")}")
    }
    options
  }
}

object SparkApplicationTemplate extends App {

  try {
    val spark = SparkSession.builder().getOrCreate()
    val sparkApplicationTemplate = new SparkApplicationTemplate(spark)
    sparkApplicationTemplate.run(args)

  } catch {
    case e: Exception =>
      e.printStackTrace()
      System.exit(1)
  }
}
