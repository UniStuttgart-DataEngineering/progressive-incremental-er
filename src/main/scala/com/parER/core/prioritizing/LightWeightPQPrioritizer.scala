package com.parER.core.prioritizing

import java.util.Comparator

import com.google.common.collect.MinMaxPriorityQueue
import com.parER.akka.streams.utils.EntityComparator
import com.parER.core.Config
import com.parER.datastructure.LightWeightComparison

import scala.collection.mutable.ListBuffer

/**
 *
 * Comparison centric approach using a Priority Queue
 * It takes the top-1 comparison from a priority queue bounded to @param MaxSize
 *
 */

class LightWeightPQPrioritizer {

  var tDequeueAll, tMedian, tPartition, tNewPQ, tNewH, tInsertPQ, tCompLoop, tUpdate, tGetTopComparisons = 0L

  // Maximum # of comparisons
  val maxSize = Int.MaxValue  // TODO check for maxvalue
  var retrieved = 0L

  var AddTime1 = 0L
  var AddTime2 = 0L

  // Priority queue
  //val pq = mutable.SortedSet()(Ordering.by[BaseComparison, Float](_.sim).reverse)
  //val pq = mutable.PriorityQueue()(Ordering.by[LightWeightComparison, Float](_.sim))

  // Max number of comparisons that a PQ can contain
  val maxComparisons = (250.0 * Config.nduplicates).toInt
  val dmaxComparisons = (1.0 * Config.nduplicates).toInt

  println(s"[LightWeightPQPrioritizer] maxComparisons = $maxComparisons")

  val comparator = new Comparator[LightWeightComparison] {
    override def compare(o1: LightWeightComparison, o2: LightWeightComparison): Int = {
      -java.lang.Float.compare(o1.sim, o2.sim)
    }
  }

  // Main pq
  val pq : MinMaxPriorityQueue[LightWeightComparison] = MinMaxPriorityQueue
    .orderedBy(comparator)
    .maximumSize(maxComparisons)
    .create()

  // Secondary pq
//  val dpq : MinMaxPriorityQueue[LightWeightComparison] = MinMaxPriorityQueue
//    .orderedBy(EntityComparator)
//    .maximumSize(dmaxComparisons)
//    .create()

  var count = 0

  // TODO search insert operations on pq are O(log(maxSize))
  def update(cmps: List[LightWeightComparison]) = {
    //var currSize = pq.size
    //count += cmps.size
    //println("Count in update: " + count)

    var t0 = System.currentTimeMillis()

    // this
//    val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / cmps.size
//    val splittedCmps = cmps.partition(x => x.sim >= w)
//    for (c <- splittedCmps._1) {
//      if (pq.size() == maxComparisons) {
//        val l = pq.peekLast()
//        if (pq.offer(c)) dpq.add(l) else dpq.add(c)
//      } else
//        pq.add(c)
//    }
//
//    AddTime1 += (System.currentTimeMillis()-t0)
//    t0 = System.currentTimeMillis()
//
//    if (splittedCmps._2.size + dpq.size < dmaxComparisons) {
//      for (c <- splittedCmps._2)
//        dpq.add(c)
//    }
    // or this
//    val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim) / cmps.size
//    for (c <- cmps.filter(_.sim >= w))
//      pq.add(c)
    // or this
    val loadPQ = pq.size().toDouble/maxComparisons
    if (loadPQ > 0.5) {
      val w = cmps.foldLeft(0.0f)( (v, c) => v + c.sim) / cmps.size
      for (c <- cmps.filter(_.sim >= w))
        pq.add(c)
    } else {
      for (c <- cmps)
        pq.add(c)
    }
    //or this
//    for (c <- cmps)
//      pq.add(c)

    AddTime2 += (System.currentTimeMillis()-t0)

//    Does not make sense this check with bounded pq
//    if (currSize + cmps.size != pq.size) {
//      println(s"error ${currSize+cmps.size} != ${pq.size}")
//      assert(currSize + cmps.size == pq.size)
//    }
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

  def isEmpty() = pq.isEmpty

  def getTopComparison()  = {
    //val c = pq.dequeue()
    //c
    pq.removeFirst()
  }

  def getTopComparisons(k: Int) = {
    val T0 = System.currentTimeMillis()
    val comparisons = ListBuffer[LightWeightComparison]()
    for (i <- 0 until k) {
      if (!isEmpty()) {
        retrieved += 1
        comparisons += getTopComparison()
      }
      // decomment if dpq is not present
//      else if (!dpq.isEmpty) {
//        retrieved += 1
//        comparisons += dpq.removeFirst()
//      }
    }
    tGetTopComparisons += (System.currentTimeMillis() - T0)
    comparisons.toList
  }

  var totSumSizes = 0L
  var totSumSizesCount = 0L
  var maxSumSize = 0L
  var countOne = 0L
  var countGreaterThanOne = 0L
  var performedComparisons = 0L

  // Return the best comparisons
  def getBestComparisons() = {
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

  def overhead() = {
    if (pq.size == 0) {
      println(s"[HPrioritizer7] Zero comparisons stored...")
    }
    println("AddTime1 (pq) in seconds: " + AddTime1/1000)
    println("AddTime1 (pq) in minutes: " + AddTime1.toDouble/(1000*60))
    println("AddTime2 (dpq) in seconds: " + AddTime2/1000)
    println("AddTime2 (dpq) in minutes: " + AddTime2.toDouble/(1000*60))
    println(s"PQ size: ${pq.size()} / ${maxComparisons}")
    println(s"PQ load factor: ${pq.size().toDouble/maxComparisons}")
  }
}
