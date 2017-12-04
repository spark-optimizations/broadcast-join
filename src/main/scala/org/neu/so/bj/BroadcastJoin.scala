package org.neu.so.bj

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * @author Tirthraj
  */
class BroadcastJoin(sc: SparkContext) extends config {
  /**
    * Return an RDD containing all pairs of elements with matching keys in `left` and `right`. Each
    * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `left` and
    * (k, v2) is in `right`. Following optimization has been added to perform inner join.
    *
    * If estimated size of `left` or `right` is less than or equal to `autoBroadcastJoinThreshold`,
    * perform custom broadcast join by grouping and broadcasting smaller sized RDD for performing map-side join
    * on larger RDD.
    */
  def join[ K: ClassTag, A: ClassTag, B: ClassTag ](left: RDD[ (K, A) ],
                                                    right: RDD[ (K, B) ],
                                                    rddSizeEstimator: RDDSizeEstimator): RDD[ (K, (A, B)) ] = {
    try {
      if (canBroadcast(left, rddSizeEstimator)) {
        println("left")
        return broadcastJoin(left, right)
      }
      else if (canBroadcast(right, rddSizeEstimator)) {
        println("right")
        return broadcastJoin(right, left).mapValues(_.swap)
      }
    }
    catch {
      case _: java.lang.Exception => println("Estimated small is not small enough to collect in memory.")
    }
    println("all")
    sc.parallelize(left.take(1)).join(sc.parallelize(right.take(1)))
  }

  /**
    * Return true iff estimated size of `rdd` is less than or equal to `autoBroadcastJoinThreshold`.
    * Referenced from: https://gist.github.com/mkolod/0662ae3e480e0a8eceda
    */
  private[ this ] def canBroadcast[ K: ClassTag, E: ClassTag ](rdd: RDD[ (K, E) ],
                                                               rddSizeEstimator: RDDSizeEstimator): Boolean = {
    val size = rddSizeEstimator.estimate(rdd)
    println("size:" + size)
    size <= autoBroadcastJoinThreshold
  }

  /**
    * Returns an RDD containing all pairs of elements with matching keys in `small` and `large`. It broadcasts
    * `small`, performs map-side join at each node with flatMap to emit each pair of values for each key.
    */
  @throws[ java.lang.OutOfMemoryError ]
  private[ this ] def broadcastJoin[ K: ClassTag, C: ClassTag, D: ClassTag ](small: RDD[ (K, C) ],
                                                                             large: RDD[ (K, D) ])
  : RDD[ (K, (C, D)) ] = {
    val s = sc.broadcast(group(small))
    large
      .flatMap {
        case (kl: K, vl: D) if s.value.contains(kl) =>
          s.value(kl).flatMap {
            case (vs: C) => Some((kl, (vs, vl)))
          }
        case _ => None
      }
  }

  /**
    * Return a Scala immutable Map by collecting `rdd` and grouping values by keys into an array of values.
    * Referenced from: https://gist.github.com/mkolod/0662ae3e480e0a8eceda
    */
  @throws[ java.lang.OutOfMemoryError ]
  private[ this ] def group[ K: ClassTag, E: ClassTag ](rdd: RDD[ (K, E) ]): Map[ K, Array[ E ] ] = {
    rdd
      .collect
      .groupBy(_._1)
      .map {
        case (k, kv) => (k, kv.map(_._2))
      }
  }
}
