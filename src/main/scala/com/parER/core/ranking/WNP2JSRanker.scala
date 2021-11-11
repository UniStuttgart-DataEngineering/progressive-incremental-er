package com.parER.core.ranking

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, ProgressiveComparison}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class WNP2JSRanker extends Ranking {

  val ccer = Config.ccer
  var count = 0

  val EntityBlockSize = List ( new mutable.HashMap[Int, Int](), new mutable.HashMap[Int, Int]() )

  override def execute(comparisons: List[BaseComparison]) = {
    if (comparisons.size == 0)
      comparisons
    else {
      val cmps = removeRedundantComparisons(comparisons)
      //val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / cmps.size
      //cmps = cmps.filter(_.sim >= w)
      cmps
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
            distinctAndCount(p._2, h.e1, _.e2, 1)
          } else {
            distinctAndCount(p._2, h.e2, _.e1, 0)
          }
        }
      }).flatten.toList
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

    val head = comparisons.head
    if (!ccer)
      distinctAndCount(comparisons, head.e2, _.e1, 0)
    else if (ccer && head.e1Model == null && head.e2Model == null)
      distinctAndCountPair(comparisons)
    else if (ccer && head.e1Model != null)
      distinctAndCount(comparisons, head.e1, _.e2, 1)
    else
      distinctAndCount(comparisons, head.e2, _.e1, 0)

  }

  private def update(e:Int, size:Int, idx: Int) = {
    EntityBlockSize(idx).update(e, size)
  }

  private def get(e:Int, idx: Int, defaultValue: Int) = {
    val v = EntityBlockSize(idx).getOrElseUpdate(e, defaultValue)
    if (v < defaultValue) {
      EntityBlockSize(idx).update(e, defaultValue)
      defaultValue
    } else
      v
  }

  private def distinctAndCount(comparisons: List[BaseComparison], entity:Int, f: BaseComparison => Int, idx: Int) = {
    if (comparisons.size == 1) {
      comparisons.head.sim = 1
      comparisons
    } else if (comparisons.size == 0) {
      comparisons
    } else {

      // Number of keys == Number of blocks
      val nKeys = comparisons.asInstanceOf[List[ProgressiveComparison]].groupBy(_.key).size
      update(entity, nKeys, idx)

      val builder = mutable.HashMap.empty[Int, BaseComparison]
      val seen = mutable.HashSet.empty[Int]
      val it = comparisons.iterator
      var different = false
      while (it.hasNext) {
        val next = it.next().asInstanceOf[ProgressiveComparison]
        val id = f(next)
        if (seen.add(id)) {
          next.sim = 1.0f
          builder(id) = next
        } else {
          builder(id).sim += 1.0f
          different = true
        }
      }

      var rankedComparisons = builder.values.toList
      //val w = rankedComparisons.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / rankedComparisons.size
      //rankedComparisons = rankedComparisons.filter(_.sim >= w)

      rankedComparisons.foreach(c => {
        val sim = c.sim
        val BSize = get(f(c),(idx+1)%2, sim.toInt)
        c.sim /= (BSize+nKeys-sim)
      })

      rankedComparisons
      //if (different) builder.values.toList else comparisons
    }
  }

  private def distinctAndCountPair(comparisons: List[BaseComparison]) = {
    if (comparisons.size == 1) {
      comparisons.head.sim = 1
      comparisons
    } else if (comparisons.size == 0) {
      comparisons
    } else {
      val cmps = comparisons.asInstanceOf[List[ProgressiveComparison]].partition(_.dID == 0)
      val cmps0 = cmps._1.groupBy(_.e1)
      val cmps1 = cmps._2.groupBy(_.e2)
      val b = List.newBuilder[BaseComparison]
      b ++= cmps0.flatMap(x => distinctAndCount(x._2, x._1, _.e2, 1))
      b ++= cmps1.flatMap(x => distinctAndCount(x._2, x._1, _.e1, 0))
      //count += b.knownSize
      //println("No of comparisons " + count)
      //println(s"${cmps._1.size} -- ${cmps._2.size} => ${b.knownSize}")
      b.result()
    }
  }
}
