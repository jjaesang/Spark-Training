package com.study.transform

import org.apache.spark.rdd.RDD

/**
  *
  * @param longestWord 교사가 쓴 모든 리포트 중 가장 긴 단어
  * @param happyMentions 해당 교사가 언급한 happy 개수
  * @param averageWords 해당 교사의 reportCard 당 단어의 평균 수
  */
case class ReportCardMetrics(
                              longestWord: Int,
                              happyMentions: Int,
                              averageWords: Double
                            )

/**
  * 이 함수는 map 연산 2번 , reduceByKey 한번 사용하는 것보다 우수
  * 왜?
  * 각 파티션을 로컬 레벨에서 합산한 다음, 타피션끼리의 리듀스를 위해 셔플을 수행
  * 그러나, 각 레코드 마다, 콤바인 함수 단계마다 우리가 만든 객체의 새로운 인스턴스를 만들어야하는 단점이 있음
  *
  * @param totalWords
  * @param longestWords
  * @param happyMentions
  * @param numberReportCards
  */
class MetricsCalculator(
                         val totalWords: Int,
                         val longestWords: Int,
                         val happyMentions: Int,
                         val numberReportCards: Int
                       ) extends Serializable {

  def sequenceOp(reportCardMetrics: String): MetricsCalculator = {
    val words = reportCardMetrics.split(" ")
    val tW = words.length
    val lW = words.map(_.length).max
    val hM = words.count(_.toLowerCase.equals("happy"))

    new MetricsCalculator(
      tW + totalWords,
      Math.max(lW, longestWords),
      hM + happyMentions,
      numberReportCards + 1
    )
  }

  def combineOp(other: MetricsCalculator): MetricsCalculator = {
    new MetricsCalculator(
      this.totalWords + other.totalWords,
      Math.max(this.totalWords, other.totalWords),
      this.happyMentions + other.happyMentions,
      this.numberReportCards + other.numberReportCards
    )
  }

  def toReportCardMetrics =
    ReportCardMetrics(
      longestWords,
      happyMentions,
      totalWords.toDouble / numberReportCards
    )


}

object NotReuseObject {

  /**
    *
    * @param rdd[(String,String)] -> (판다 교사, 리포트 카드 문자열)
    * @return 판다 교사, 리포트 카드의 통계, ReportCardMetrics case class가진 통계값
    *
    */
  def calculateReportCardStatistics(rdd: RDD[(String, String)]): RDD[(String, ReportCardMetrics)] = {
    rdd.aggregateByKey(new MetricsCalculator(totalWords = 0,
      longestWords = 0, happyMentions = 0, numberReportCards = 0))(
      seqOp = ((reportCardMetrics, reportCardText) => reportCardMetrics.sequenceOp(reportCardText)),
      combOp = (x, y) => x.combineOp(y))
      .mapValues(_.toReportCardMetrics)
  }
}
