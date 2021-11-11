package com.parER.core.compcleaning

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable

class GlobalHSCompCleaner extends HSCompCleaner {

  val seen = mutable.HashSet.empty[(Int, Int)]

  override def execute(comparisons: List[BaseComparison]) = {
    if (comparisons.size == 0)
      comparisons
    else {
      removeRedundantComparisons(comparisons)
    }
  }

  override def removeRedundantComparisons(comparisons: List[BaseComparison]) = {
    // In Dirty ER: for all c in BaseComparison c.e2 is equal
    if (!ccer | comparisons.head.e2 == comparisons.last.e2)
      distinctAndCount(comparisons, _.e1, 0)
    else if (ccer && comparisons.head.e1 == comparisons.last.e1)
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
      val builder = mutable.ListBuffer.empty[BaseComparison]
      val it = comparisons.iterator
      var different = false
      while (it.hasNext) {
        val next = it.next()
        val id = f(next)
        if ( seen.add((next.e1, next.e2)) ) {
          next.sim = 1
          builder.addOne(next)
        } else {
          different = true
        }
      }
      if (different) builder.toList else comparisons
    }
  }

//  private def distinctAndCount(comparisons: List[BaseComparison], f: BaseComparison => Int, idx: Int) = {
//    if (comparisons.size == 1) {
//      comparisons.head.counters(0) = 1
//      comparisons
//    } else if (comparisons.size == 0) {
//      comparisons
//    } else {
//      val builder = mutable.HashMap.empty[Int, BaseComparison]
//      val seen = mutable.HashSet.empty[Int]
//      val it = comparisons.iterator
//      var different = false
//      while (it.hasNext) {
//        val next = it.next()
//        val id = f(next)
//        if (seen.add(id)) {
//          next.counters(idx) = 1
//          builder(id) = next
//        } else {
//          builder(id).counters(idx) += 1
//          different = true
//        }
//      }
//      if (different) builder.values.toList else comparisons
//    }
//  }
}
