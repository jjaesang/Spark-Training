package com.study.goldilocks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.Map
/**
  * Created by jaesang on 2019-06-14.
  */
object GoldilocksGroupByKey {

  def mapToKeyValuePairs(dataFrame: DataFrame): RDD[(Int, Double)] = {
    val rowLength = dataFrame.schema.length
    dataFrame.rdd.flatMap(
      row => Range(0, rowLength).map(i => (i, row.getDouble(i)))
    )
  }

  def findRankStatistics(dataFrame: DataFrame,
                         ranks: List[Long]): Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))

    val pairRDD: RDD[(Int, Double)] = mapToKeyValuePairs(dataFrame)

    val groupColumns: RDD[(Int, Iterable[Double])] = pairRDD.groupByKey()
    groupColumns.mapValues(
      iter => {
        val sortedIter = iter.toArray.sorted

        sortedIter.toIterable.zipWithIndex.flatMap({
          case (colValue, index) =>
            if (ranks.contains(index + 1)) {
              Iterator(colValue)
            } else {
              Iterator.empty
            }
        })
      }).collectAsMap()
  }

  def findRankStatistics(pairRDD: RDD[(Int, Double)],
                         ranks: List[Long]): Map[Int, Iterable[Double]] = {
    assert(ranks.forall(_ > 0))
    pairRDD.groupByKey().mapValues(iter => {
      val sortedIter = iter.toArray.sorted
      sortedIter.zipWithIndex.flatMap(
        {
          case (colValue, index) =>
            if (ranks.contains(index + 1)) {
              //this is one of the desired rank statistics
              Iterator(colValue)
            } else {
              Iterator.empty
            }
        }
      ).toIterable //convert to more generic iterable type to match out spec
    }).collectAsMap()
  }

}
