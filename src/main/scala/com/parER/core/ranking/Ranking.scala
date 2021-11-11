package com.parER.core.ranking

import com.parER.core.compcleaning.{CNPCompCleaner, ComparisonCleaning, HSCompCleaner, WNP2CompCleaner, WNPCompCleaner}
import com.parER.datastructure.BaseComparison

// TODO all the rankers need to normalize values in a range between 0 and 1
trait Ranking {
  def execute(comparisons: List[BaseComparison]) : List[BaseComparison]
}

object Ranking {
  def apply(name: String) = name match {
    case "js" => new WNP2JSRanker
    case "cbs" => new WNP2CBSMaxRanker
  }
}