package org.neu.so.bj

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * @author Tirthraj
  */
object BroadcastJoinSuite {
  val ss: SparkSession = createSparkSession()
  val sc: SparkContext = createSparkContext()

  def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Broadcast Join Test Suite")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
  }

  def createSparkContext(): SparkContext = {
    val sc = ss.sparkContext
    sc.setLogLevel("ERROR")
    sc
  }

  def main(args: Array[ String ]): Unit = {
    val testData = fetchTestData(args(0) + "similar_artists.csv")
    val smallRDD = extractSmallRDD(testData)
    //    broadcastJoinFail(testData, smallRDD, args(1) + "bj_fail_output")
    broadcastJoinExec(testData, smallRDD, args(1) + "bj_output")
    //    dataframeJoinExec(testData, smallRDD, args(1) + "df_output")
    //    normalJoinExec(testData, smallRDD, args(1) + "normal_rdd_output")
    //    broadcastJoinExec(testData, testData, args(1) + "big_broadcast_output")
    //    normalJoinExec(testData, testData, args(1) + "big_normal_output")
  }

  def extractSmallRDD[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ]): RDD[ (K, V) ] = {
    sc.parallelize(rdd.take(200))
  }

  def fetchTestData[ K: ClassTag, V: ClassTag ](inputFile: String): RDD[ (String, String) ] = {
    sc.textFile(inputFile)
      .mapPartitionsWithIndex {
        case (0, iter) => iter.drop(1)
        case (_, iter) => iter
      }
      .map(line => (line.split(";")(0), line))
  }


  def broadcastJoinExec[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ], smallRDD: RDD[ (K, V) ],
                                                    outputPath: String): Unit = {
    val bj = new BroadcastJoin(sc)
    timeBlock(
      bj.join(rdd, smallRDD, new RDDSizeEstimator {})
        .coalesce(1, shuffle = false)
        .saveAsTextFile(outputPath)
    )
  }

  def timeBlock[ F ](block: => F): Unit = {
    val startTime = System.currentTimeMillis
    block
    println("Normal RDD Join: " + (System.currentTimeMillis - startTime) / 1000f + "s\n")
  }

  def broadcastJoinFail[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ], smallRDD: RDD[ (K, V) ],
                                                    outputPath: String): Unit = {
    val bj = new BroadcastJoin(sc)
    bj.autoBroadcastJoinThreshold = Long.MaxValue
    timeBlock(
      bj.join(rdd, rdd, new RDDSizeEstimator {})
        .coalesce(1, shuffle = false)
        .saveAsTextFile(outputPath)
    )
  }

  def dataframeJoinExec(rdd: RDD[ (String, String) ], smallRDD: RDD[ (String, String) ],
                        outputPath: String): Unit = {
    import ss.implicits._
    val df = rdd.toDF("1", "2")
    val smallDF = smallRDD.toDF("3", "4")
    timeBlock(
      df.join(smallDF, df.col("1") === smallDF.col("3"))
        .write
        .save(outputPath)
    )
  }

  def normalJoinExec[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ], smallRDD: RDD[ (K, V) ],
                                                 outputPath: String): Unit = {
    timeBlock(
      rdd.join(smallRDD)
        .coalesce(1, shuffle = false)
        .saveAsTextFile(outputPath)
    )
  }
}
