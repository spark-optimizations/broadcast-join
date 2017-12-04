package org.neu.so.bj

import java.io.{FileWriter, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator

import scala.reflect.ClassTag

/**
  * @author Tirthraj
  */
trait RDDSizeEstimator {
  def estimate[ T: ClassTag ](rdd: RDD[ T ]): Long = {
    getTotalSize(rdd)
  }

  /**
    * Returns estimated size of `rdd` by using SizeEstimator for collected single row and multiplying it with
    * total number of rows.
    */
  private[ this ] def getTotalSize[ T: ClassTag ](rdd: RDD[ T ]): Long = {
    val totalRows = rdd.count()
    val partitionWiseRowSize: Array[ Long ] = getPartitionWiseRowSize(rdd)
    val avgRowSize: Long = (partitionWiseRowSize.sum / partitionWiseRowSize.length).ceil.toLong
    totalRows * avgRowSize
  }

  private[ this ] def getPartitionWiseRowSize[ T: ClassTag ](rdd: RDD[ T ]): Array[ Long ] = {
    rdd
      .mapPartitions {
        iterator =>
          val row = iterator.take(1).toList
          Seq(SizeEstimator.estimate(row.map(_.asInstanceOf[ AnyRef ]))).iterator
      }
      .collect()
  }
}
