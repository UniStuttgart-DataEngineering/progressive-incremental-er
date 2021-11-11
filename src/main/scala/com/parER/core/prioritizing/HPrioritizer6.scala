package com.parER.core.prioritizing

import com.parER.datastructure.{BaseComparison, Comparison}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *
 * Comparison centric approach for prioritizer using a Tree Map. As illustrated in the paper.
 *
 */

class HPrioritizer6 extends Prioritizing {

  type SimpleComparison = (Int, Int, Float)

  val TIME0 = System.currentTimeMillis()

  // Time variables
  var refineOH = 0.0f
  var refineEmptyOH = 0.0f
  var refineOHInUpdate = 0.0f
  var tDequeueAll, tMedian, tPartition, tNewPQ, tNewH, tInsertPQ, tCompLoop, tUpdate, tGetTopComparisons = 0L
  var tMinAfter = 0L
  var tUpdateSet = 0L
  var tUpdatePQ = 0L
  var tPruneComparisons = 0L

  // Comparison counters
  var inserted = 0L
  var discarded = 0L
  var totPQ = 0.0f
  var retrieved = 0L

  // Constants
  val MAX_KEY = Float.MaxValue
  val maxComparisons = 5e8

  // For comparison similarity threshold computation
  var tot = 0.0f
  var max_sim = 0.0f
  var count = 0L

  // H and PQ
  val H = new mutable.TreeMap[Float, ListBuffer[SimpleComparison]]()
  var PQ = mutable.PriorityQueue()(Ordering.by[SimpleComparison, Float](_._3))

  // H[(0.0f, MAX_KEY] initialized
  H(MAX_KEY) = ListBuffer[SimpleComparison]()

  def free() = {
  }

  def refineEmptyH() = {
    val t0 = System.currentTimeMillis()
    H.maxBefore(MAX_KEY) match {
      case Some(p) => {
        PQ ++= p._2
        H.remove(p._1)
      }
    }
    refineEmptyOH += (System.currentTimeMillis()-t0)
  }

  def refineH() = {
    val max : Float = PQ.head._3
    val min : Float = H.maxBefore(MAX_KEY) match {
      case Some(p) => p._1
      case None => 0.0f
    }
    val m : Float = min + (max-min)/2.0f
    if (min != max) {
      PQ.partition(x => x._3 > m ) match {
        case (left,right) => {
          PQ = left
          H(m) = ListBuffer[SimpleComparison]() ++ right
        }
      }
    }
  }

  override def update(cmps: List[BaseComparison]) = {
    var t0, T0 = System.currentTimeMillis()
    for (c <- cmps) {
      val sim = c.sim.toFloat
      tot += c.sim
      count += 1

      H.minAfter(sim) match {
        case None => println("H: ERRORE")
        case Some(p) => {
          if (p._1 != MAX_KEY) {
            if (sim > (tot/count)) {
              p._2.addOne((c.e1, c.e2, sim))
            } else {
              discarded += 1
            }
          } else {
            totPQ += c.sim
            PQ.enqueue((c.e1, c.e2, sim))
          }
        }
      }
    }
    tCompLoop += (System.currentTimeMillis()-t0)

    t0 = System.currentTimeMillis()
    val avgPQ = totPQ/PQ.size
    if (PQ.isEmpty) {
      totPQ = 0.0f
    } else if (2 * avgPQ < PQ.head._3) {
      refineH()
      totPQ = PQ.foldRight(0.0f)((a,b)=>a._3+b)
    }
    refineOHInUpdate += (System.currentTimeMillis()-t0)
    refineOH += (System.currentTimeMillis()-t0)

    t0 = System.currentTimeMillis()
    if (H.size > 2 && maxComparisons < (count - discarded - retrieved)) {
      H.minAfter(0.0f) match {
        case Some(p) => {
          discarded += p._2.size
          H.remove(p._1)
        }
      }
    }
    tPruneComparisons += (System.currentTimeMillis()-t0)
    tUpdate += (System.currentTimeMillis() - T0)
  }

  def size() = {
    PQ.size
  }

  override def isEmpty() = {
    if (PQ.isEmpty && H.size > 1) {
      refineEmptyH()
      isEmpty()
    } else
      PQ.isEmpty
  }

  def refineIfEmpty() = {
    if (PQ.isEmpty && H.size > 1)
      refineEmptyH()
  }

  def getTopComparison() : BaseComparison = {
    val c = PQ.dequeue()
    Comparison(c._1, null, c._2, null)
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
      retrieved += 1
      List(getTopComparison())
    } else
      List()
  }

  def getTopComparisons(k: Int) = {
    val T0 = System.currentTimeMillis()
    val comparisons = ListBuffer[BaseComparison]()
    for (i <- 0 until k) {
      if (!isEmpty()) {
        retrieved += 1
        comparisons += getTopComparison()
      }
    }
    tGetTopComparisons += (System.currentTimeMillis() - T0)
    comparisons.toList
  }

  def getAvg() = totSumSizes.toFloat / totSumSizesCount.toFloat
  def getMax() = maxSumSize
  def getOnes = countOne
  def getGreaterThanOne = countGreaterThanOne

  def getNComparisons() = performedComparisons
  def getAvgNComparisons() = performedComparisons.toFloat / totSumSizesCount.toFloat
  def overhead() = {
    println(s"[data] H.size = ${H.size}")
    val MAXRANGE = H.maxBefore(MAX_KEY) match {
      case Some(p) => p._1
      case None => -1.0f
    }
    println(s"[HPrioritizer6: data] PQ size = ${PQ.size}")
    if (PQ.size > 0)
      println(s"[HPrioritizer6: data] PQ.top.sim = ${PQ.head._3}")
    println(s"[HPrioritizer6: data] max range = (${MAXRANGE}, MAX_KEY)")
    println(s"[HPrioritizer6: data] thresh cut off ${tot/count.toFloat}")
    println(s"[HPrioritizer6: data] Count ${count}")
    println(s"[HPrioritizer6: data] Discarded ${discarded}")
    println(s"[HPrioritizer6: data] Retrieved ${retrieved}")
    println(s"[HPrioritizer6: data] Now inside ${count - discarded - retrieved}")

    println(s"[HPrioritizer6: time] tComploop (s) = ${tCompLoop/1000} (NOTE tMinAfter, tUpdateSet and tUpdatePQ are included here)")
    println(s"[HPrioritizer6: time] tUpdate (s) = ${tUpdate/1000} ")
    println(s"[HPrioritizer6: time] tRefineOH (s) = ${refineOH/1000} ")
    println(s"[HPrioritizer6: time] tRefineEmptyOH (s) = ${refineEmptyOH/1000} ")
    println(s"[HPrioritizer6: time] tRefineOHInUpdate (s) = ${refineOHInUpdate/1000} ")
    println(s"[HPrioritizer6: time] tPruneComparisons (s) = ${tPruneComparisons/1000} ")
    println(s"[HPrioritizer6: time] getTopComparisons (s) = ${tGetTopComparisons/1000} ")

//    println(s"[update] Refine H overhead: ${refineOH/1000} s \n[update] Refine empty H overhead: ${refineEmptyOH/1000} s")
//    println(s"[update] tMinAfter (s) = ${tMinAfter/1000}")
//    println(s"[update] tUpdateSet (s) = ${tUpdateSet/1000}")
//    println(s"[update] tUpdatePQ (s) = ${tUpdatePQ/1000}")
//    println(s"[update] tComploop (s) = ${tCompLoop/1000} (NOTE tMinAfter, tUpdateSet and tUpdatePQ are included here)")
//    println(s"[update] tPrunedComparisons (s) = ${tPruneComparisons/1000}")
//    println(s"[update] tSum (s) = ${(refineOH+tMinAfter+tUpdateSet+tUpdatePQ)/1000}")
//    println(s"[refineH] tDequeueAll (s) = ${tDequeueAll/1000}")
//    println(s"[refineH] tMedian (s) = ${tMedian/1000}")
//    println(s"[refineH] tPartition (s) = ${tPartition/1000}")
//    println(s"[refineH] tNewPQ (s) = ${tNewPQ/1000}")
//    println(s"[refineH] tInsertPQ (s) = ${tInsertPQ/1000}")
//    println(s"[refineH] tNewH (s) = ${tNewH/1000}")
  }
}
