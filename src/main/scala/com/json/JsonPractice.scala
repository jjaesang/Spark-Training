package com.json

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

/**
  * Created by jaesang on 2019-04-29.
  */
object JsonPractice {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val json = Seq(
      ("{\n  \"id\": \"5cc695379fc7cb19aec19791\",\n  \"adInfo\": [\n    {\n      \"ad\": \"Leila\",\n      \"time\": [\n        \"Francisca\",\n        \"Lott\",\n        \"Frankie\"\n      ]\n    },\n    {\n      \"ad\": \"Becky\",\n      \"time\": [\n        \"Maricela\",\n        \"Ballard\",\n        \"Allie\"\n      ]\n    },\n    {\n      \"ad\": \"Norton\",\n      \"time\": [\n        \"Le\",\n        \"Dalton\",\n        \"Henry\"\n      ]\n    }\n  ]\n}"),
      ("{\n  \"id\": \"5cc6954a36200e5fe8936315\",\n  \"adInfo\": [\n    {\n      \"ad\": \"Casandra\",\n      \"time\": [\n        \"Ashley\",\n        \"Kathrine\",\n        \"Lesa\"\n      ]\n    },\n    {\n      \"ad\": \"Ray\",\n      \"time\": [\n        \"Iva\",\n        \"Dawn\",\n        \"Nichols\"\n      ]\n    },\n    {\n      \"ad\": \"Tyson\",\n      \"time\": [\n        \"Calderon\",\n        \"Stephenson\",\n        \"Mabel\"\n      ]\n    }\n  ]\n}")
    ).toDF("json_data")

    val schema = StructType(
      List(
        StructField("id", StringType),
        StructField("adInfo", ArrayType(
          StructType(
            List(
              StructField("ad", StringType),
              StructField("time", ArrayType(StringType))
            )
          )
        ))
      )
    )
    schema.printTreeString()

    import org.apache.spark.sql.functions._
    val newJson = json.withColumn("JSON", from_json($"json_data", schema))
    newJson.select($"JSON.adInfo.ad").show(false)


  }
}
