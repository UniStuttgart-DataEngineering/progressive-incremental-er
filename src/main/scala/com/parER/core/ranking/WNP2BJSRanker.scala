package com.parER.core.ranking

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, LightWeightComparison, ProgressiveComparison}

import scala.collection.mutable

class WNP2BJSRanker extends Ranking {

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

  def executeMessageComparisons(idx: Int, dId: Int, keyToComparisons: Seq[(String, List[LightWeightComparison])], f: LightWeightComparison => Int) = {
    var cBlocker = 0
    for (s <- keyToComparisons) {
      cBlocker += s._2.size
    }
    if (keyToComparisons.size == 0) {
      update(idx, 0, dId)
      List[LightWeightComparison]()
    } else {
      val nKeys = keyToComparisons.size
      update(idx, nKeys, dId)
      //var cmps = keyToComparisons.flatMap(_._2).toList
      var cmps = removeRedundantMessageComparisons(keyToComparisons, f)
      val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / cmps.size
      cmps = cmps.filter(_.sim >= w)
      cmps.foreach(c => {
        val sim = c.sim.toInt
        val BSize = get(f(c),(dId+1)%2, sim)
        c.sim /= (BSize+nKeys-sim).toFloat
        c.sim *= 100.0f
      })
      assert(cmps.size <= cBlocker)
      cmps
    }
  }

  def removeRedundantMessageComparisons(comparisons: Seq[(String, List[LightWeightComparison])], f: LightWeightComparison => Int) = {

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

  private def distinctAndCountMessageComparisons(comparisons: Seq[(String, List[LightWeightComparison])], f: LightWeightComparison => Int) = {
    if (comparisons.size == 1) {
      for (c <- comparisons.head._2)
        c.sim = 1
      comparisons.head._2
      //comparisons.head.sim = 1
      //comparisons
    } else if (comparisons.size == 0) {
      List[LightWeightComparison]()
    } else {
      val builder = mutable.HashMap.empty[Int, LightWeightComparison]
      val seen = mutable.HashSet.empty[Int]
      val sit = comparisons.iterator
      var different = false
      while (sit.hasNext) {
        val it = sit.next()._2.iterator
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
      }
      builder.values.toList// else comparisons.flatMap(_._2).toList
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

  private def update(e:Int, size:Int, dId: Int) = {
    EntityBlockSize(dId).update(e, size)
  }

  private def get(e:Int, dId: Int, defaultValue: Int) = {
    val v = EntityBlockSize(dId).getOrElseUpdate(e, defaultValue)
    if (v < defaultValue) {
      EntityBlockSize(dId).update(e, defaultValue)
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
        c.sim /= (BSize+nKeys-sim).toFloat
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
