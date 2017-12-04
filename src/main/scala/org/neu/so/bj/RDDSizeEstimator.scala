package org.neu.so.bj

import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator

import scala.reflect.ClassTag

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
    println("Total rows: " + totalRows)
    val rowSize: Long = SizeEstimator.estimate(Seq(rdd.take(1)).map(_.asInstanceOf[ AnyRef ]))
    totalRows * rowSize
  }
}
