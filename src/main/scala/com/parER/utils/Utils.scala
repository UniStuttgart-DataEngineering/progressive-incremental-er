package com.parER.utils

import com.parER.datastructure.{BaseComparison, Comparison}

object Utils {
  def printComparisons(comparisons: List[BaseComparison]) = {
    var buf = new StringBuilder
    for (cmp <- comparisons) {
      buf ++= "(" + cmp.e1 + "," + cmp.e2 +  "|" + cmp.sim + ")"
    }
    println(buf)
  }

  def measureTime(start: Long, end: Long, info: String) = {
    println("(" + info + ") Elapsed time: " + (end - start) + "ms")
  }

  def trunc(x: Float, n: Int) = {
    def p10(n: Int, pow: Long = 10): Long = if (n==0) pow else p10(n-1,pow*10)
    if (n < 0) {
      val m = p10(-n).toFloat
      math.round(x/m) * m
    }
    else {
      val m = p10(n).toFloat
      math.round(x*m) / m
    }
  }

  def assertOrder(comparisons: List[BaseComparison]) = {
    var l = comparisons.head.sim
    for (c <- comparisons) {
      assert( l >= c.sim )
      l = c.sim
    }
  }
}
