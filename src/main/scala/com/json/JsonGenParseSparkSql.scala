package com.json

import com.json.template.{InterestInfo, InterestLists}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by jaesang on 2019-04-29.
  */

object JsonGenParseSparkSql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val schema = StructType(
      List(
        StructField("id", StringType),
        StructField("InterestInfos", ArrayType(
          StructType(
            List(
              StructField("interest", StringType),
              StructField("time", ArrayType(StringType)),
              StructField("isActive", BooleanType)
            )
          )
        ))
      )
    )

    val sampleJsonData = Seq(
      InterestLists("jaesang", Seq(
        InterestInfo("spark", Seq("190428", "190429", "1904230"), true),
        InterestInfo("hadoop", Seq("190302", "190320"), false)
      )),
      InterestLists("kkj", Seq(
        InterestInfo("tensorflow", Seq("190428", "190429", "1904230"), true),
        InterestInfo("pytorch", Seq("190302", "190420"), true)
      ))).toDF()

    val json = sampleJsonData.toJSON
      .withColumn("json_format", from_json($"value", schema))

    // json.printSchema()
    /**
      *
      * root
      * |-- value: string (nullable = true)
      * |-- json_format: struct (nullable = true)
      * |    |-- id: string (nullable = true)
      * |    |-- InterestInfos: array (nullable = true)
      * |    |    |-- element: struct (containsNull = true)
      * |    |    |    |-- interest: string (nullable = true)
      * |    |    |    |-- time: array (nullable = true)
      * |    |    |    |    |-- element: string (containsNull = true)
      * |    |    |    |-- isActive: boolean (nullable = true)
      *
      */

    val ret = json.select($"json_format".getItem("id").as("id"),
      explode($"json_format".getItem("InterestInfos")).as("interests"))
      .withColumn("hot_interest", when($"interests".getItem("isActive"), $"interests".getItem("interest")).otherwise(""))
      .groupBy($"id")
      .agg(collect_set($"hot_interest").as("interestResult"))
      .select($"id", concat_ws(",", $"interestResult").as("interest"))

    ret.printSchema()
    /**
      * root
       |-- id: string (nullable = true)
       |-- interest: string (nullable = false)
     */

    ret.show(false)
    /**
      * +-------+------------------+
        |id     |interest          |
        +-------+------------------+
        |jaesang|spark,            |
        |kkj    |pytorch,tensorflow|
        +-------+------------------+
      */


  }
}
