package org.neu.so.bj

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object BroadcastJoinSuite {
  val sc: SparkContext = createContext()
  sc.setLogLevel("ERROR")
  val bj = new BroadcastJoin(sc)

  def main(args: Array[String]): Unit = {
    val inputFile = args(0) + "song_info.csv"
    val rdd1 = sc.textFile(inputFile)
      .mapPartitionsWithIndex {
        case (0, iter) => iter.drop(1)
        case (_, iter) => iter
      }
      .map(line => (line.split(";")(0), line))

    val rdd2 = sc.parallelize(rdd1.take(200))

    val rdd3 = rdd1.partitionBy(new HashPartitioner(100))

    var startTime = System.currentTimeMillis
    bj.join(rdd2, rdd1)
      .coalesce(1, shuffle = false)
      .saveAsTextFile(args(1) + "bj_output")
    println("Broadcast Join: " + (System.currentTimeMillis - startTime) / 1000f + "s\n")

    startTime = System.currentTimeMillis
    rdd3.join(rdd2)
      .coalesce(1, shuffle = false)
      .saveAsTextFile(args(1) + "HashPartitioner_output")
    println("HashPartitioner Join: " + (System.currentTimeMillis - startTime) / 1000f + "s\n")

    startTime = System.currentTimeMillis
    rdd1.join(rdd2)
      .coalesce(1, shuffle = false)
      .saveAsTextFile(args(1) + "normal_output")
    println("Normal Join: " + (System.currentTimeMillis - startTime) / 1000f + "s\n")
  }

  def createContext(): SparkContext = {
    val conf = new SparkConf().setAppName("Broadcast Join Test Suite").setMaster("local")
    new SparkContext(conf)
  }
}
