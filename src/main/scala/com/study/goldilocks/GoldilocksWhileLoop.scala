package com.study.goldilocks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by jaesang on 2019-06-14.
  */
object GoldilocksWhileLoop {
  /**
    * dataFrame:
    * (0.0, 4.5, 7.7, 5.0)
    * (1.0, 5.5, 6.7, 6.0)
    * (2.0, 5.5, 1.5, 7.0)
    * (3.0, 5.5, 0.5, 7.0)
    * (4.0, 5.5, 0.5, 8.0)
    *
    * ranks:
    * 1, 3
    *
    */
  def findRankStatistic(dataFrame: DataFrame, ranks: List[Long]): Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))

    val numberOfColumns = dataFrame.schema.length
    var i = 0
    var result = Map[Int, Iterable[Double]]()

    while (i < numberOfColumns) {
      val col = dataFrame.rdd.map(row => row.getDouble(i))
      val sortedCol: RDD[(Double, Long)] = col.sortBy(v => v).zipWithIndex()
      val rankOnly = sortedCol.filter({
        case (colValue, index) => ranks.contains(index + 1)
      }).keys

      val list = rankOnly.collect()
      result += (i -> list)
      i += 1
    }
    result
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("GoldilocksWhileLoop")
      .getOrCreate()

    import spark.implicits._

    val dataFrame: DataFrame = Seq(
      (0.0, 4.5, 7.7, 5.0),
      (1.0, 5.5, 6.7, 6.0),
      (2.0, 5.5, 1.5, 7.0),
      (3.0, 5.5, 0.5, 7.0),
      (4.0, 5.5, 0.5, 8.0)
    ).toDF()

    val ranks: List[Long] = List(1, 3)
    val rankStatistic = findRankStatistic(dataFrame, ranks)
    rankStatistic.foreach(row => {
      val idx = row._1
      val statisticInfo = row._2
      println(s"$idx - ${statisticInfo.mkString(",")}")
    })
  }

}
