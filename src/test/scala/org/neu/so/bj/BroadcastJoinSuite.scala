package org.neu.so.bj

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.neu.so.bj.util.{SparkUtil, TestUtil}

import scala.reflect.ClassTag

/**
  * @author Tirthraj
  */
object BroadcastJoinSuite {
  val ss: SparkSession = SparkUtil.createSparkSession()
  val sc: SparkContext = SparkUtil.createSparkContext(ss)

  def main(args: Array[ String ]): Unit = {
    val testData = fetchTestData(args(0) + "similar_artists.csv.gz")
    val smallRDD = extractSmallRDD(testData)
    broadcastJoinExec(testData, smallRDD, args(1) + "bj_output", "bj_output")
    dataframeJoinExec(testData, smallRDD, args(1) + "df_output")
    normalJoinExec(testData, smallRDD, args(1) + "shuffle_output")
    broadcastJoinExec(testData, testData, args(1) + "big_broadcast_output", "big_broadcast_output")
    normalJoinExec(testData, testData, args(1) + "big_normal_output")
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
                                                    outputPath: String,
                                                    statsFile: String): Unit = {
    val bj = new BroadcastJoin(sc)
    bj.statsPath = "output/" + statsFile
    TestUtil.timeBlock(
      bj.join(rdd, smallRDD, new RDDSizeEstimator {})
        .coalesce(1, shuffle = false)
        .saveAsTextFile(outputPath),
      "BJ: "
    )
  }

  def dataframeJoinExec(rdd: RDD[ (String, String) ], smallRDD: RDD[ (String, String) ],
                        outputPath: String): Unit = {
    import ss.implicits._
    val df = rdd.toDF("1", "2")
    val smallDF = smallRDD.toDF("3", "4")
    TestUtil.timeBlock(
      df.join(smallDF, df.col("1") === smallDF.col("3"))
        .write
        .save(outputPath),
      "DF: "
    )
  }

  def normalJoinExec[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ], smallRDD: RDD[ (K, V) ],
                                                 outputPath: String): Unit = {
    TestUtil.timeBlock(
      rdd.join(smallRDD)
        .coalesce(1, shuffle = false)
        .saveAsTextFile(outputPath),
      "Shuffled Join: "
    )
  }
}
