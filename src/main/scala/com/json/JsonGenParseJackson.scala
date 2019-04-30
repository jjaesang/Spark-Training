package com.json

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.json.template.{InterestInfo, InterestLists}
import org.apache.spark.sql.SparkSession

/**
  * Created by jaesang on 2019-04-29.
  */

object JsonGenParseJackson {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val sampleJsonData = Seq(
      InterestLists("jaesang", Seq(
        InterestInfo("spark", Seq("190428", "190429", "1904230"), true),
        InterestInfo("hadoop", Seq("190302", "190320"), false)
      )),
      InterestLists("kkj", Seq(
        InterestInfo("tensorflow", Seq("190428", "190429", "1904230"), true),
        InterestInfo("pytorch", Seq("190302", "190420"), true)
      ))).toDF()

    sampleJsonData.show(false)

    val json = sampleJsonData.toJSON.mapPartitions(row => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)

      row.flatMap(record => {
        try {
          Some(mapper.readValue(record, classOf[InterestLists]))
        } catch {
          case e: Exception => {
            e.printStackTrace()
            None
          }
        }
      })
    })

    json.map(row => {
      val id = row.id
      val interests = row.InterestInfos.filter(_.isActive).map(_.interest).mkString(",")
      (id,interests)
    }).show(false)

  }
}
