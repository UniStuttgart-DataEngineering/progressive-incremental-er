package com.parER.core.prioritizing

import com.parER.datastructure.BaseComparison

import scala.Option
import scala.collection.mutable

/**
 *
 * Comparison centric approach for prioritizer using a Tree Map. As illustrated in the paper.
 *
 */

class HPrioritizer extends Prioritizing {

  // TODO use plain priority queue for 1.0f insertions.

  def getSortedSet() = mutable.SortedSet[BaseComparison]()(Ordering.by[BaseComparison, (Float, (Int, Int))](x => (x.sim, (x.e1, x.e2))).reverse)
  def refineEmptyH() = {
    //println("refineEmpty")
    val (k, v) = H.maxBefore(1.0f) match { case Some(p) => p }
    H(1.0f).addAll(v)
    H.remove(k)
  }

  def refineH() = {
    //println("Refine H... " + H.size)
    val s = H(1.0f).asInstanceOf[mutable.SortedSet[BaseComparison]]
    val l = s.toList

    // TODO assert first is higher or equal than last
    assert(l(0).sim >= l.last.sim)

    var m = l(Math.ceil(l.length/2).toInt).sim
    if (m == l(0).sim) {
      m = l.foldLeft(0.0f)((x,y)=>x+y.sim) / l.size
    }
    if (m != l(0).sim) {
      val p = l.partition(x => x.sim > m)

      //println(l(0).sim + "----" + l.last.sim + "----" + m + s"=====(${p._1.size}), ${p._2.size}")

      // TODO what if similar values?

      if (p._1.last.sim == p._2(0).sim)
        assert(p._1.last.sim != p._2(0).sim)

      H.remove(1.0f)
      H(1.0f) = getSortedSet() ++ p._1
      H(m) = new mutable.HashSet[BaseComparison]() ++ p._2
    }
  }

  val maxSize = 10000
  val H = new mutable.TreeMap[Float, mutable.Set[BaseComparison]]()
  H(1.0f) = getSortedSet()

  //var count = 0

  override def update(cmps: List[BaseComparison]) = {
    for (c <- cmps) {
      // TODO assumption that all the cmps have sim <= 1
      assert(c.sim <= 1.0f)
      val x = H.minAfter(c.sim)
      x match {
        case None => println("H: ERRORE")
        case Some(p) => {
          val k = p._1
          val s = p._2
          s += c
          if (k == 1.0f && maxSize < s.size) {
            refineH()
          }
        }
      }
    }
  }

  def size() = {
    // TODO modify
    H(1.0f).size
  }

  override def isEmpty() = {
    val s = H(1.0f)
    if (s.isEmpty && H.size > 1) {
      refineEmptyH()
      isEmpty()
    } else
      s.isEmpty
  }

  def refineIfEmpty() = {
    val s = H(1.0f).asInstanceOf[mutable.SortedSet[BaseComparison]]
    if (s.isEmpty && H.size > 1)
      refineEmptyH()
  }

  def getTopComparison() : BaseComparison = {
    val s = H(1.0f).asInstanceOf[mutable.SortedSet[BaseComparison]]
    val c = s.firstKey
    s.remove(c)
    c
  }

  var totSumSizes = 0L
  var totSumSizesCount = 0L
  var maxSumSize = 0L
  var countOne = 0L
  var countGreaterThanOne = 0L
  var performedComparisons = 0L

  // Return the best comparisons
  override def getBestComparisons() = {
    if (!isEmpty()) {
      //assert(pq.head.sim >= pq.last.sim) //TODO remove
      List(getTopComparison())
    } else
      List()
  }

  def getAvg() = totSumSizes.toFloat / totSumSizesCount.toFloat
  def getMax() = maxSumSize
  def getOnes = countOne
  def getGreaterThanOne = countGreaterThanOne

  def getNComparisons() = performedComparisons
  def getAvgNComparisons() = performedComparisons.toFloat / totSumSizesCount.toFloat

}
