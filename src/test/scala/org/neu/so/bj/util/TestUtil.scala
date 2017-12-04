package org.neu.so.bj.util

object TestUtil {
  def timeBlock[ F ](block: => F, msg: String): Unit = {
    val startTime = System.currentTimeMillis
    block
    println(msg + (System.currentTimeMillis - startTime) / 1000f + "s\n")
  }
}
