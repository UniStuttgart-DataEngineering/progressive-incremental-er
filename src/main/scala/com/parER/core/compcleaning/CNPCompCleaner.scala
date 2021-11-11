package com.parER.core.compcleaning

import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.textmodels.TokenNGrams

class CNPCompCleaner extends HSCompCleaner {

  override def execute(comparisons: List[BaseComparison]) = {
    if (comparisons.size == 0)
      comparisons
    else {
      val n : Float = comparisons.size
      var cmps = removeRedundantComparisons(comparisons).sortWith(_.sim > _.sim)
      val d : Float = cmps.size
      val i = (n/d - 1).ceil.toInt
      println(s"n=${n} -- d=${d} -- i=${i} ")
      cmps = cmps.splitAt(i)._1
      cmps
    }
  }

  override def execute(id: Int, model: TokenNGrams, ids: List[Int]): (Int, TokenNGrams, List[Int]) = (id, model, ids)
}
