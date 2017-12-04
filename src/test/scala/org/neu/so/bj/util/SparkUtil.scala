package org.neu.so.bj.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.neu.so.bj.BroadcastJoinSuite.ss

object SparkUtil {
  def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Broadcast Join Test Suite")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
  }

  def createSparkContext(ss: SparkSession): SparkContext = {
    val sc = ss.sparkContext
    sc.setLogLevel("ERROR")
    sc
  }
}
