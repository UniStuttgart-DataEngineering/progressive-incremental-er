package com.parER.core.prioritizing

import com.parER.datastructure.{BaseComparison, Comparison}

trait Prioritizer {
  def execute(comparisons: List[BaseComparison]) : Unit
  def execute(comparisons: List[BaseComparison], threshold: Float) : List[BaseComparison]
  def get(): List[BaseComparison]
  def get(k : Int): List[BaseComparison]
  def hasComparisons(): Boolean
}

object Prioritizer {
  def apply(name: String, opt: Long = 0) = name match {
    case "no" => new NoPrioritizer
    case "pps" => new PpsPrioritizer(opt.toInt)
    case "bf" => new BFPrioritizer(opt, 1)
  }
}