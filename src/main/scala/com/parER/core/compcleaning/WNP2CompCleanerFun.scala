package com.parER.core.compcleaning

import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.textmodels.TokenNGrams

object WNP2CompCleanerFun extends HSCompCleaner {

  override final def execute(comparisons: List[BaseComparison]) = {
    if (comparisons.size == 0)
      comparisons
    else {
      var cmps = removeRedundantComparisons(comparisons)
      val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / cmps.size
      cmps = cmps.filter(_.sim >= w)
      cmps
    }
  }

  override final def execute(id: Int, model: TokenNGrams, ids: List[Int]): (Int, TokenNGrams, List[Int]) = {
    if (ids.size == 0) {
      (id, model, ids)
    } else {
      var hm = removeRedundantIntegers(ids)
      val w = hm.values.foldLeft(0.0f)( (v, c) => v + c.toFloat) / hm.values.size
      (id, model, hm.filter(_._2.toFloat >= w).keys.toList)
    }
  }

}
