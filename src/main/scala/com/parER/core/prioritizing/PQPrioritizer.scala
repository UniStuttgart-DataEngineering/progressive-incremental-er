package com.parER.core.prioritizing

import com.parER.core.Config
import com.parER.datastructure.BaseComparison

import scala.collection.mutable

/**
 *
 * Comparison centric approach using a Priority Queue
 * It takes the top-1 comparison from a priority queue bounded to @param MaxSize
 *
 */

class PQPrioritizer extends Prioritizing {

  // Maximum # of comparisons
  val maxSize = Int.MaxValue  // TODO check for maxvalue

  // Priority queue
  //val pq = mutable.SortedSet()(Ordering.by[BaseComparison, Float](_.sim).reverse)
  val pq = mutable.PriorityQueue()(Ordering.by[BaseComparison, Float](_.sim))

  var count = 0

  // TODO search insert operations on pq are O(log(maxSize))
  override def update(cmps: List[BaseComparison]) = {
    var currSize = pq.size
    //count += cmps.size
    //println("Count in update: " + count)

    for (c <- cmps)
      pq += c

    if (currSize + cmps.size != pq.size) {
      println("error")
      assert(currSize + cmps.size == pq.size)
    }
    //pq += c
//    cmps.foreach(c => {
//      if (pq.size > maxSize) {
//        if (c.sim > pq.last.sim) {
//          pq -= pq.last
//          pq += c
//        }
//      } else
//        pq += c
//    })
  }

  def size() = {
    pq.size
  }

  override def isEmpty() = pq.isEmpty

  def getTopComparison() : BaseComparison = {
    val c = pq.dequeue()
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
    if (!pq.isEmpty) {
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
