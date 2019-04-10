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
  * MetrixCalculator의 단계마다 객체의 새로운 인스턴스를 만드는 단점을 해결
  * 매 단계마다 새 객체를 돌려주는 것이 아닌, sequence연산은 원본 누적변수를 수정해서 반환
  * combine연산은 첫번째 누적 변수를 수정해서 되돌려줌
  *
  * 즉, this.type 패러다임을 적용
  *
  * @param totalWords
  * @param longestWords
  * @param happyMentions
  * @param numberReportCards
  */
class MetricsCalculatorReuseObject(
                                    var totalWords: Int,
                                    var longestWords: Int,
                                    var happyMentions: Int,
                                    var numberReportCards: Int
                                  ) extends Serializable {

  def sequenceOp(reportCardMetrics: String): this.type = {
    val words = reportCardMetrics.split(" ")
    totalWords += words.length
    longestWords = Math.max(longestWords, words.map(_.length).max)
    happyMentions += words.count(_.toLowerCase().equals("happy"))
    numberReportCards += 1
    this
  }

  def combineOp(other: MetricsCalculatorReuseObject): this.type = {
    totalWords += other.totalWords
    longestWords = Math.max(this.longestWords, other.longestWords)
    happyMentions += other.happyMentions
    numberReportCards += other.numberReportCards
    this
  }

  def toReportCardMetrics =
    ReportCardMetrics(
      longestWords,
      happyMentions,
      totalWords.toDouble / numberReportCards
    )


}

object ReuseObject {

  /**
    *
    * @param rdd[(String,String)] -> (판다 교사, 리포트 카드 문자열)
    * @return 판다 교사, 리포트 카드의 통계, ReportCardMetrics case class가진 통계값
    *
    */
  def calculateReportCardStatistics(rdd: RDD[(String, String)]): RDD[(String, ReportCardMetrics)] = {
    rdd.aggregateByKey(new MetricsCalculator(totalWords = 0,
      longestWords = 0, happyMentions = 0, numberReportCards = 0))(
      seqOp = (reportCardMetrics, reportCardText) => reportCardMetrics.sequenceOp(reportCardText),
      combOp = (x, y) => x.combineOp(y))
      .mapValues(_.toReportCardMetrics)
  }
}
