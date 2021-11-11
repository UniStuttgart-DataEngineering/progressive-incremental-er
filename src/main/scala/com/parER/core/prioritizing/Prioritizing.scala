package com.parER.core.prioritizing

import com.parER.core.ranking.{WNP2CBSMaxRanker, WNP2JSRanker}
import com.parER.datastructure.BaseComparison

trait Prioritizing {
  def update(cmps: List[BaseComparison])
  def getBestComparisons() : List[BaseComparison]
  def isEmpty() : Boolean
}

object Prioritizing {
  def apply(name: String) = name match {
    case "tm" => new TreeMapPrioritizer
    case "etm" => new ExtendedTreeMapPrioritizer
    case "pq" => new PQPrioritizer
    case "pq5" => new PQKPrioritizer(5)
    case "pq10" => new PQKPrioritizer(10)
  }
}