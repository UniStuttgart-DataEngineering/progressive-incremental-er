package com.parER.core.prioritizing

import java.util.Comparator

import com.google.common.collect.MinMaxPriorityQueue
import com.parER.core.Config
import com.parER.datastructure.LightWeightComparison

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *
 * Comparison centric approach for prioritizer using a Tree Map. As illustrated in the paper.
 *
 */

class HPrioritizer8 {

  type SimpleComparison = LightWeightComparison//(Int, Int, Float)

  def getSim(sc: SimpleComparison) = sc.sim.toFloat

  val TIME0 = System.currentTimeMillis()
  var tNow = TIME0

  // Time variables
  var refineOH = 0.0f
  var refineEmptyOH = 0.0f
  var refineOHInUpdate = 0.0f
  var tRefineOHINside = 0.0f
  var tAvgComputation = 0.0f
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
  val refineHInterval = if (Config.nduplicates > 100000) 60 else 1

  // For comparison similarity threshold computation
  var tot = 0.0f
  var max_sim = 0.0f
  var count = 0L

  // Max number of comparisons that a PQ can contain
  val maxComparisons = 10 * Config.nduplicates
  var maxComparisonsInMax = maxComparisons
  var divisor = 1

  println(s"[HPrioritizer8] maxComparisons = $maxComparisons")

  val comparator = new Comparator[LightWeightComparison] {
    override def compare(o1: LightWeightComparison, o2: LightWeightComparison): Int = {
      -java.lang.Float.compare(o1.sim, o2.sim)
    }
  }

  // H and PQ
  var maxHSize = 0
  val H = new mutable.TreeMap[Float, MinMaxPriorityQueue[LightWeightComparison]]()

  // H[(0.0f, MAX_KEY] initialized
  H(MAX_KEY) = MinMaxPriorityQueue
    .orderedBy(comparator)
    .maximumSize(maxComparisons/divisor)
    .create()

  divisor *= 10

  // For pruning
  var pruning = true

  def free() = {
  }

  def refineEmptyH() = {
    val t0 = System.currentTimeMillis()
    H.maxBefore(MAX_KEY) match {
      case Some(p) => {
        H.remove(p._1)
        H(MAX_KEY) = p._2
        println(s"[HPrioritizer8] remove PQ!")
      }
    }
    refineEmptyOH += (System.currentTimeMillis()-t0)
  }

  var refineHCountInside, refineHCountInLoop, refineHCountMinMax = 0L
  def refineH() = {
    val t0 = System.currentTimeMillis()
    refineHCountInside += 1
    val max : Float = getSim( H(MAX_KEY).peekFirst() )
    val min : Float = H.maxBefore(MAX_KEY) match {
      case Some(p) => p._1
      case None => 0.0f
    }
    val m : Float = min + (max-min)/2.0f
    if (min != max && maxComparisonsInMax > 100) {
      maxComparisonsInMax = maxComparisons/divisor
      refineHCountMinMax += 1
      H(m) = MinMaxPriorityQueue
        .orderedBy(comparator)
        .maximumSize(maxComparisonsInMax)
        .create()
      println(s"[HPrioritizer8] new PQ!")
      divisor *= 10

      while( H(MAX_KEY).peekLast().sim < m ) {
        H(m).add( H(MAX_KEY).removeLast() )
      }

    }
    tRefineOHINside += (System.currentTimeMillis()-t0)
  }

  def update(cmps: List[LightWeightComparison]) = {
    var t0, T0 = System.currentTimeMillis()
    for (c <- cmps) {
      val sim = c.sim.toFloat
      if (pruning) {
        tot += c.sim
        count += 1
      }

      H.minAfter(sim) match {
        case None => println("H: ERRORE")
        case Some(p) => {
          if (p._1 != MAX_KEY) {
            if (!pruning || sim > (tot/count)) {
              p._2.add(c)
              //p._2.addOne((c.e1, c.e2, sim))
            } else {
              discarded += 1
            }
          } else {
            //totPQ += c.sim
            H(MAX_KEY).add(c)
          }
        }
      }
    }
    tCompLoop += (System.currentTimeMillis()-t0)
    //t0 = System.currentTimeMillis()
    if ( H(MAX_KEY).size > 0.8 * maxComparisonsInMax) {
      refineH()
      //tNow = t0
    }

    refineOHInUpdate += (System.currentTimeMillis()-t0)
    refineOH += (System.currentTimeMillis()-t0)

//    t0 = System.currentTimeMillis()
//    if (pruning && H.size > 2 && maxComparisons < (count - discarded - retrieved)) {
//      H.minAfter(0.0f) match {
//        case Some(p) => {
//          discarded += p._2.size
//          H.remove(p._1)
//        }
//      }
//    }
//
//    maxHSize = Math.max(maxHSize, H.size)
//    tPruneComparisons += (System.currentTimeMillis()-t0)
//    tUpdate += (System.currentTimeMillis() - T0)
  }

//  def refine() = {
//    var t0 = System.currentTimeMillis()
//    val avgPQ = totPQ/H(MAX_KEY).size
//    if (H(MAX_KEY).isEmpty) {
//      totPQ = 0.0f
//      // con 5e6 completa stream in 20min
//    } else if (1e6 < H(MAX_KEY).size && 1.5 * avgPQ < getSim( H(MAX_KEY).head )) {
//      refineH()
//      totPQ = H(MAX_KEY).foldRight(0.0f)((a,b)=>getSim( a ) +b)
//    }
//    refineOHInUpdate += (System.currentTimeMillis()-t0)
//    refineOH += (System.currentTimeMillis()-t0)
//
//    t0 = System.currentTimeMillis()
//    if (H.size > 2 && maxComparisons < (count - discarded - retrieved)) {
//      H.minAfter(0.0f) match {
//        case Some(p) => {
//          discarded += p._2.size
//          H.remove(p._1)
//        }
//      }
//    }
//    tPruneComparisons += (System.currentTimeMillis()-t0)
//  }

  def size() = {
    H(MAX_KEY).size
  }

  def isEmpty() : Boolean = {
    if (H(MAX_KEY).isEmpty && H.size > 1) {
      refineEmptyH()
      isEmpty()
    } else
      H(MAX_KEY).isEmpty
  }

  def refineIfEmpty() = {
    if (H(MAX_KEY).isEmpty && H.size > 1)
      refineEmptyH()
  }

  def getTopComparison(range: Float) : LightWeightComparison = {
    H(range).removeFirst()
  }

  var totSumSizes = 0L
  var totSumSizesCount = 0L
  var maxSumSize = 0L
  var countOne = 0L
  var countGreaterThanOne = 0L
  var performedComparisons = 0L

  // Return the best comparisons
  def getBestComparisons() = {
    if (!isEmpty()) {
      retrieved += 1
      List(getTopComparison(MAX_KEY))
    } else
      List()
  }

  def getTopComparisons(k: Int) = {
    val T0 = System.currentTimeMillis()
    val comparisons = ListBuffer[LightWeightComparison]()
    var range = MAX_KEY
    for (i <- 0 until k) {
      while (range != -1.0f && H(range).isEmpty) {
        range = H.maxBefore(range) match {
                case Some(p) => p._1
                case None => -1.0f
        }
      }
      if (range != -1.0f)
        comparisons += getTopComparison(range)
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
    if (count - discarded - retrieved == 0) {
      println(s"[HPrioritizer7] Zero comparisons stored...")
    }
//    println("===========================")
//    println(s"[HPrioritizer7] H.size = ${H.size}")
//    println(s"[HPrioritizer7] Max H.size = $maxHSize")
//    val MAXRANGE = H.maxBefore(MAX_KEY) match {
//      case Some(p) => p._1
//      case None => -1.0f
//    }
//    println(s"[HPrioritizer6: data] PQ size = ${H(MAX_KEY).size}")
//    if (H(MAX_KEY).size > 0)
//      println(s"[HPrioritizer7: data] PQ.top.sim = ${getSim ( H(MAX_KEY).head )}")
//    println(s"[HPrioritizer7: data] max range = (${MAXRANGE}, MAX_KEY)")
//    println(s"[HPrioritizer7: data] thresh cut off ${tot/count.toFloat}")
//    println(s"[HPrioritizer7: data] Count ${count}")
//    println(s"[HPrioritizer7: data] Discarded ${discarded}")
//    println(s"[HPrioritizer7: data] Retrieved ${retrieved}")
//    println(s"[HPrioritizer7: data] Now inside ${count - discarded - retrieved}")
//
//    println(s"[HPrioritizer7: data] refineHCountInLoop ${refineHCountInLoop}")
//    println(s"[HPrioritizer7: data] refineHCountInside ${refineHCountInside}")
//    println(s"[HPrioritizer7: data] refineHCountInside ${refineHCountInside}")
//    println(s"[HPrioritizer7: time] tComploop (s) = ${tCompLoop/1000} (NOTE tMinAfter, tUpdateSet and tUpdatePQ are included here)")
//    println(s"[HPrioritizer7: time] tUpdate (s) = ${tUpdate/1000} ")
//    println(s"[HPrioritizer7: time] tRefineOH (s) = ${refineOH/1000} ")
//    println(s"[HPrioritizer7: time] tAvgComputation (s) = ${tAvgComputation/1000} ")
//    println(s"[HPrioritizer7: time] tRefineEmptyOH (s) = ${refineEmptyOH/1000} ")
//    println(s"[HPrioritizer7: time] tRefineOHInUpdate (s) = ${refineOHInUpdate/1000} ")
//    println(s"[HPrioritizer7: time] tRefineOHINside (s) = ${tRefineOHINside/1000} ")
//    println(s"[HPrioritizer7: time] tPruneComparisons (s) = ${tPruneComparisons/1000} ")
//    println(s"[HPrioritizer7: time] getTopComparisons (s) = ${tGetTopComparisons/1000} ")

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
