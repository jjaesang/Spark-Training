package com.study.job

import org.apache.spark.sql.SparkSession

/**
  * Created by jaesang on 2019-03-14.
  */

case class JobOption(
                      name: String = "SparkBatchJob",
                      confType: String = "db",
                      jobId: Int = -1,
                      confFile: String = ""
                    )

object JobLauncher extends Logger {

  def parseArguments(args: Array[String]): JobOption = {

    val parser = new scopt.OptionParser[JobOption]("run") {
      // parsing arguments..
    }

    parser.parse(args, JobOption()) match {
      case Some(o) => o
      case None =>
        parser.showUsage()
        throw new IllegalArgumentException(s"failed to parse options... (${args.mkString(",")}")
    }

  }

  def main(args: Array[String]): Unit = {

    val options = parseArguments(args)
    logger.info(s"Job Options : ${options}")

    val jobDescription = JobDescription()

    val spark = SparkSession
      .builder()
      .getOrCreate()

    val job = new Job(spark, jobDescription)
    job.run()

  }

}
