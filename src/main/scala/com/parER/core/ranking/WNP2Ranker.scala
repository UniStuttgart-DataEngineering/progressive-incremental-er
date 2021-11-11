package com.parER.core.ranking

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, ProgressiveComparison}

import scala.collection.mutable

class WNP2Ranker extends Ranking {

  val ccer = Config.ccer

  override def execute(comparisons: List[BaseComparison]) = {
    if (comparisons.size == 0)
      comparisons
    else {
      var cmps = removeRedundantComparisons(comparisons)
      val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / cmps.size
      cmps = cmps.filter(_.sim >= w)
      cmps
    }
  }

  // TODO possible bug in CC procedures, entity with id 37
  def removeRedundantComparisons(comparisons: List[BaseComparison]) = {
    // OLD MAYBE INCORRECT
    // In Dirty ER: for all c in BaseComparison c.e2 is equal
//    if (!ccer | comparisons.head.e2 == comparisons.last.e2)
//      distinctAndCount(comparisons, _.e1, 0)
//    else if (ccer && comparisons.head.e1 == comparisons.last.e1)
//      distinctAndCount(comparisons, _.e2, 1)
//    else
//      distinctAndCount(comparisons, _.e1, 0)

    // In Dirty ER: for all c in BaseComparison c.e2 is equal
    if (!ccer)
      distinctAndCount(comparisons, _.e1, 0)
    else if (ccer && comparisons.head.e1Model != null)
      distinctAndCount(comparisons, _.e2, 1)
    else
      distinctAndCount(comparisons, _.e1, 0)
  }

  private def distinctAndCount(comparisons: List[BaseComparison], f: BaseComparison => Int, idx: Int) = {
    if (comparisons.size == 1) {
      comparisons.head.sim = 1
      comparisons
    } else if (comparisons.size == 0) {
      comparisons
    } else {
      val nKeys = comparisons.asInstanceOf[List[ProgressiveComparison]].groupBy(_.key).size
      val builder = mutable.HashMap.empty[Int, BaseComparison]
      val seen = mutable.HashSet.empty[Int]
      val it = comparisons.iterator
      var different = false
      while (it.hasNext) {
        val next = it.next().asInstanceOf[ProgressiveComparison]
        val id = f(next)
        if (seen.add(id)) {
          next.sim = 1.0f / nKeys
          builder(id) = next
        } else {
          builder(id).sim += (1.0f / nKeys.toFloat)
          different = true
        }
      }
      if (different) builder.values.toList else comparisons
    }
  }

}
