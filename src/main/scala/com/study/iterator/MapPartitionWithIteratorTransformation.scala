package com.study.iterator

import org.apache.spark.rdd.RDD

import scala.collection.mutable

class MapPartitionWithIteratorTransformation {

  private def asIteratorToIteratorTransformation(
                                                  valueColumnPairIters: Iterator[((Double, Int), Long)],
                                                  targetsInThisPart: List[(Int, Long)]
                                                ): Iterator[(Int, Double)] = {

    val columnRelativeIndex: Map[Int, List[Long]] = targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
    val columnInThisPart : List[Int] = targetsInThisPart.map(_._1).distinct

    val runningTotals: mutable.HashMap[Int, Long] = new mutable.HashMap()
    runningTotals ++= columnInThisPart.map(columnIndex => (columnIndex, 1L)).toMap

    val pairWithRanksInThisParts = valueColumnPairIters.filter {
      case ((value, colIndex), count) => columnInThisPart.contains(colIndex)
    }

    pairWithRanksInThisParts.flatMap {
      case ((value, colIndex), count) => {

        val total = runningTotals(colIndex)
        val ranksPresent: List[Long] = columnRelativeIndex(colIndex)
          .filter(index => (index <= count + total) && (index > total))

        val nextElems: Iterator[(Int, Double)]
        = ranksPresent.map(r => (colIndex, value)).toIterator

        runningTotals.update(colIndex, total + count)
        nextElems
      }
    }


  }


  private def findTargetRanksIteratively(
                                          sortedAggrateValueColumnPairs: RDD[((Double, Int), Long)],
                                          ranksLocations: Array[(Int, List[(Int, Long)])]
                                        ): RDD[(Int, Double)] = {

    sortedAggrateValueColumnPairs.mapPartitionsWithIndex(
      (partitionIndex: Int, aggreateValueColumnPairs: Iterator[((Double, Int), Long)]) => {

        val targetInThisPart: List[(Int, Long)] = ranksLocations(partitionIndex)._2
        if (targetInThisPart.nonEmpty) {
          asIteratorToIteratorTransformation(aggreateValueColumnPairs, targetInThisPart)
        }
        else
          Iterator.empty
      }
    )
  }

}
