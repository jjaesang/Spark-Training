package com.study.job

import org.apache.spark.sql.SparkSession


/**
  * Created by jaesang on 2019-03-14.
  */


case class JobOption(
                      input: String = "input_path",
                      output: String = "output_path",
                      jobId: Int = -1,
                      confFile: String = ""
                    )

object JobLauncher extends Logger {

  def parseArguments(args: Array[String]): JobOption = {

    val parser = new scopt.OptionParser[JobOption]("run") {
      head("scala scopt test code ", "3.x")
      opt[String]('i', "input") required() valueName ("<input-path>") action {
        (arg, config) => config.copy(input = arg)
      } text "input is the input path"

      opt[String]('o', "output") required() valueName ("<output-path>") action {
        (arg, config) => config.copy(output = arg)
      } text "output is the output path"

    }

    val options = parser.parse(args, JobOption()) match {
      case Some(o) => o
      case None =>
        parser.showUsage
        throw new IllegalArgumentException(s"failed to parse options... (${args.mkString(",")}")
    }
    options

  }

  def main(args: Array[String]): Unit = {

    val options = parseArguments(args)

    logger.info(s"Job Options : ${options}")
    println(options.toString)

    // val jobDescription = JobDescription()

    val spark = SparkSession
      .builder()
      .getOrCreate()

    val job = new Job(spark)
    job.run()

  }

}
