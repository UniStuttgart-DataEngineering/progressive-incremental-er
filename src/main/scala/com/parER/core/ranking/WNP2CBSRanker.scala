package com.parER.core.ranking

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, LightWeightComparison, ProgressiveComparison}
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable

class WNP2CBSRanker extends Ranking {

  val ccer = Config.ccer

  override def execute(comparisons: List[BaseComparison]) = {
    if (comparisons.size == 0)
      comparisons
    else {
      var cmps = removeRedundantComparisons(comparisons)
//      val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / cmps.size
//      cmps = cmps.filter(_.sim >= w)
//      cmps
      removeRedundantComparisons(comparisons)
    }
  }

  def executeWithFunc(comparisons: List[BaseComparison], f: BaseComparison => Int) = {
    if (comparisons.size == 0)
      comparisons
    else {
      val cmps = removeRedundantComparisonsWithFunc(comparisons, f)
      val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / cmps.size
      cmps.filter(_.sim >= w)
    }
  }

  def executeMessageComparisons(comparisons: List[LightWeightComparison], f: LightWeightComparison => Int) = {
    if (comparisons.size == 0)
      comparisons
    else {
      //this
      removeRedundantMessageComparisons(comparisons, f)
      // or this
//      val cmps = removeRedundantMessageComparisons(comparisons, f)
//      val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / cmps.size
//      cmps.filter(_.sim >= w)
    }
  }

  def executeSplittedMessageComparisons(comparisons: List[LightWeightComparison], f: LightWeightComparison => Int) = {
    if (comparisons.size == 0)
      (comparisons, comparisons)
    else {
      val cmps = removeRedundantMessageComparisons(comparisons, f)
      val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / cmps.size
      //cmps.filter(_.sim >= w)
      cmps.partition(x => x.sim >= w)
    }
  }

  def executePairs(pairs: Seq[(Int, List[BaseComparison])]) = {
    if (pairs.size == 0) {
      List[BaseComparison]()
    } else {
      pairs.map(p => {
        if (p._2.size == 0) {
          p._2
        } else {
          val h = p._2.head
          if (p._1 == 0) {  // dId == 0
            distinctAndCount(p._2, _.e2)
          } else {
            distinctAndCount(p._2, _.e1)
          }
        }
      }).flatten.toList
    }
  }

  def removeRedundantComparisons(comparisons: List[BaseComparison]) = {

    // In Dirty ER: for all c in BaseComparison c.e2 is equal
    val head = comparisons.head
    if (!ccer)
      distinctAndCount(comparisons, _.e1)
    //else if (ccer && head.e1Model == null && head.e2Model == null)
    //  distinctAndCountPair(comparisons)
    else if (ccer && head.e1Model != null)
      distinctAndCount(comparisons, _.e2)
    else
      distinctAndCount(comparisons, _.e1)
  }

  def removeRedundantMessageComparisons(comparisons: List[LightWeightComparison], f: LightWeightComparison => Int) = {

    // In Dirty ER: for all c in BaseComparison c.e2 is equal
    val head = comparisons.head
    if (!ccer)
      distinctAndCountMessageComparisons(comparisons, _.e1)
    else //if (ccer && head.e1Model == null && head.e2Model == null)
      distinctAndCountMessageComparisons(comparisons, f)
//    else if (ccer && head.e1Model != null)
//      distinctAndCount(comparisons, _.e2)
//    else
//      distinctAndCount(comparisons, _.e1)
  }

  def removeRedundantComparisonsWithFunc(comparisons: List[BaseComparison], f: BaseComparison => Int) = {

    // In Dirty ER: for all c in BaseComparison c.e2 is equal
    val head = comparisons.head
    if (!ccer)
      distinctAndCount(comparisons, _.e1)
    else if (ccer && head.e1Model == null && head.e2Model == null)
      distinctAndCount(comparisons, f)
    else if (ccer && head.e1Model != null)
      distinctAndCount(comparisons, _.e2)
    else
      distinctAndCount(comparisons, _.e1)
  }

//  private def distinctAndCountPair(comparisons: List[BaseComparison]) = {
//    if (comparisons.size == 1) {
//      comparisons.head.sim = 1
//      comparisons
//    } else if (comparisons.size == 0) {
//      comparisons
//    } else {
//      val cmps = comparisons.asInstanceOf[List[ProgressiveComparison]].partition(_.dID == 0)
//      val cmps0 = cmps._1.groupBy(_.e1)
//      val cmps1 = cmps._2.groupBy(_.e2)
//      val b = List.newBuilder[BaseComparison]
//      b ++= cmps0.flatMap(x => distinctAndCount(x._2, x._1, _.e2, 1))
//      b ++= cmps1.flatMap(x => distinctAndCount(x._2, x._1, _.e1, 0))
//      //count += b.knownSize
//      //println("No of comparisons " + count)
//      //println(s"${cmps._1.size} -- ${cmps._2.size} => ${b.knownSize}")
//      b.result()
//    }
//  }

  //private def distinctAndCount(comparisons: List[BaseComparison], f: BaseComparison => Int, idx: Int) = {
  private def distinctAndCount(comparisons: List[BaseComparison], f: BaseComparison => Int) = {
    if (comparisons.size == 1) {
      comparisons.head.sim = 1
      comparisons
    } else if (comparisons.size == 0) {
      comparisons
    } else {
      val builder = mutable.HashMap.empty[Int, BaseComparison]
      val seen = mutable.HashSet.empty[Int]
      val it = comparisons.iterator
      var different = false
      while (it.hasNext) {
        val next = it.next()
        val id = f(next)
        if (seen.add(id)) {
          next.sim = 1
          builder(id) = next
        } else {
          builder(id).sim += 1.0f
          different = true
        }
      }
      if (different) builder.values.toList else comparisons
    }
  }

  private def distinctAndCountMessageComparisons(comparisons: List[LightWeightComparison], f: LightWeightComparison => Int) = {
    if (comparisons.size == 1) {
      comparisons.head.sim = 1
      comparisons
    } else if (comparisons.size == 0) {
      comparisons
    } else {
      val builder = mutable.HashMap.empty[Int, LightWeightComparison]
      val seen = mutable.HashSet.empty[Int]
      val it = comparisons.iterator
      var different = false
      while (it.hasNext) {
        val next = it.next()
        val id = f(next)
        if (seen.add(id)) {
          next.sim = 1
          builder(id) = next
        } else {
          builder(id).sim += 1.0f
          different = true
        }
      }
      if (different) builder.values.toList else comparisons
    }
  }

}
