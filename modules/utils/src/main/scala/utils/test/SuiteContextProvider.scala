package utils.test

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SuiteContextProvider {
  @transient var _spark: SparkSession = _

  def getOptSparkSession: Option[SparkSession] = Option(_spark)

  def getOptSparkContext: Option[SparkContext] = getOptSparkSession.map(_.sparkContext)
}
