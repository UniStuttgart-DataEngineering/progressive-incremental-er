package com.parER.core.prioritizing

import com.parER.datastructure.BaseComparison

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *
 * Comparison centric approach for prioritizer using a Tree Map. As illustrated in the paper.
 *
 */

class HPrioritizer5 extends Prioritizing {

  val TIME0 = System.currentTimeMillis()

  def free() = {
    if (H.size > 1) {
      println("H size: " + H.size)
      println("Distribution: " + H.map(_._2.size).toList)
      println("Tot number of comparisons: " + H.map(_._2).foldLeft(0.0f)((a,b) => a + b.size))

      //var (k, v) = H.minAfter(0.0f) match { case Some(p) => p }
      //v = null
      //H.update(k, new mutable.HashSet[BaseComparison]())
      //println("Memory free in HPrioritizer3: success")
    }
  }


  var refineOH = 0.0f
  var refineEmptyOH = 0.0f
  val MAX_KEY = Float.MaxValue

  // TODO use plain priority queue for 1.0f insertions.

  //def getSortedSet() = mutable.SortedSet[BaseComparison]()(Ordering.by[BaseComparison, (Float, (Int, Int))](x => (x.sim, (x.e1, x.e2))).reverse)
  def refineEmptyH() = {
    //println("refineEmpty...")
    val t0 = System.currentTimeMillis()
    val (k, v) = H.maxBefore(MAX_KEY) match { case Some(p) => p }
    //println(s"filling PQ with ${v.size} comparisons")
    PQ ++= v
    H.remove(k)
    var delta = (System.currentTimeMillis()-t0)
    refineEmptyOH += delta
    //println("End RefineEmptyH... " + delta + " ms")
  }

  var tDequeueAll, tMedian, tPartition, tNewPQ, tNewH, tInsertPQ, tCompLoop = 0L

  def refineH() = {

    var t0 = System.currentTimeMillis()
    // TODO assert first is higher or equal than last
    //assert(l(0).sim >= l.last.sim)
//    println("Refine H... " + H.size)
//    println("H size: " + H.size)
//    println("Distribution: " + H.map(_._2.size).toList)
//    println("PQ size " + PQ.size)
//    println("Tot number of comparisons: " + H.map(_._2).foldLeft(0.0f)((a,b) => a + b.size))
    //val l = PQ.toList //PQ.dequeueAll.toList
    tDequeueAll += (System.currentTimeMillis()-t0)

    t0 = System.currentTimeMillis()
    val max : Float = PQ.head.sim
    val min : Float = H.maxBefore(MAX_KEY) match {
      case Some(p) => p._1
      case None => 0.0f
    }
    val m : Float = min + (max-min)/2.0f

    //val (c, min, max) = l.foldLeft((0.0f, 1.0f, 0.0f))((x,y) => (x._1 + y.sim, if (x._2 < y.sim) x._2 else y.sim, if (x._3 > y.sim) x._3 else y.sim))
    //val m = c / l.size.toFloat // TODO (min+(max-min)/2)
    tMedian += (System.currentTimeMillis()-t0)

    if (min != max) {

      t0 = System.currentTimeMillis()
      val p = PQ.partition(x => x.sim > m)
      tPartition += (System.currentTimeMillis()-t0)
      //println("Refine H... " + H.size + " and " + l.size)
      //println(l(0).sim + "----" + l.last.sim + "----" + m + s"=====(${p._1.size}), ${p._2.size}")

      // TODO what if similar values?
      //if (p._1.last.sim == p._2(0).sim)
      //  assert(p._1.last.sim != p._2(0).sim)

      //H.remove(1.0f)
      //H(1.0f) = getSortedSet() ++ p._1
      t0 = System.currentTimeMillis()
      PQ = p._1
      tNewPQ += (System.currentTimeMillis()-t0)

      t0 = System.currentTimeMillis()
      H(m) = new ListBuffer[BaseComparison]() ++ p._2
      tNewH += (System.currentTimeMillis()-t0)

    } else {
      t0 = System.currentTimeMillis()
      //PQ ++= l
      tInsertPQ += (System.currentTimeMillis()-t0)
    }

  }

  val maxComparisons = 1e8
  val maxSize = 1e6
  val H = new mutable.TreeMap[Float, ListBuffer[BaseComparison]]()
  var PQ = mutable.PriorityQueue()(Ordering.by[BaseComparison, Float](_.sim))
  H(MAX_KEY) = ListBuffer[BaseComparison]()

  var count = 0L
  var inserted = 0L
  var discarded = 0L
  var tMinAfter = 0L
  var tUpdateSet = 0L
  var tUpdatePQ = 0L
  var tPruneComparisons = 0L

  def updateSet(s: mutable.Set[BaseComparison], c: BaseComparison) = {
    val t = System.currentTimeMillis()
    s += c
    tUpdateSet += (System.currentTimeMillis()-t)
  }

  def updateList(s: ListBuffer[BaseComparison], c: BaseComparison) = {
    val t = System.currentTimeMillis()
    s += c
    tUpdateSet += (System.currentTimeMillis()-t)
  }

  var totPQ = 0.0f

  def updatePQ(c: BaseComparison) = {
    val t = System.currentTimeMillis()
    totPQ += c.sim
    PQ.enqueue(c)
    tUpdatePQ += (System.currentTimeMillis()-t)
  }

  var tot = 0.0f
  var max_sim = 0.0f

  override def update(cmps: List[BaseComparison]) = {
    var t0 = System.currentTimeMillis()
    for (c <- cmps) {

      tot += c.sim
      count += 1

      var t = System.currentTimeMillis()
      val x = H.minAfter(c.sim)
      tMinAfter += (System.currentTimeMillis()-t)

      x match {
        case None => println("H: ERRORE")
        case Some(p) => {
          val k = p._1
          val s = p._2
          if (k != MAX_KEY) {
            if (c.sim > (tot/count.toFloat)) {
              inserted += 1
              updateList(s,c) //s += c
            } else {
              discarded += 1
            }
          } else
            inserted += 1
            updatePQ(c) //PQ.enqueue(c)
        }
      }
    }
    tCompLoop += (System.currentTimeMillis()-t0)

    t0 = System.currentTimeMillis()
    val avgPQ = totPQ/PQ.size
    if (PQ.isEmpty) {
      //println("[PQ.isEmpty] PQ is empty...")
      //println(s"[PQ.isEmpty] H.size = ${H.size}")
      //println(s"[PQ.isEmpty] totPQ = ${totPQ}")
      totPQ = 0.0f
    } else if (2 * avgPQ < PQ.head.sim) {
      refineH()
      totPQ = PQ.foldRight(0.0f)((a,b)=>a.sim+b)
    }
    refineOH += (System.currentTimeMillis()-t0)

    t0 = System.currentTimeMillis()
//    if (H.size > 2 && maxComparisons < count - discarded) {
//      println("AAAAAAAAAAAAAAAAAA -> REMOVING COMPARISONS ")
//      H.minAfter(0.0f) match {
//        case Some(p) => {
//          println("AAAAAAAAAAAAAAAAAA -> REMOVING COMPARISONS ")
//          var (k, s) = (p._1, p._2)
//          inserted -= s.size
//          count -= s.size
//          H.remove(k)
//          s = null
//        }
//      }
//    }
    if (H.size > 3 && maxComparisons < count - discarded) {
      //println("maxComparisons < count - discarded")
      //println(s"$maxComparisons < $count - $discarded")
      var (k, s) = H.minAfter(0.0f) match {
        case Some(p) => (p._1, p._2)
      }
      count -= s.size
      H.remove(k)
      s = null
    }
    tPruneComparisons += (System.currentTimeMillis()-t0)

  }

  def size() = {
    // TODO modify
    PQ.size
  }

  override def isEmpty() = {
    //val s = H(1.0f)
    if (PQ.isEmpty && H.size > 1) {
      refineEmptyH()
      isEmpty()
    } else
      PQ.isEmpty
  }

  def refineIfEmpty() = {
    //val s = H(1.0f).asInstanceOf[mutable.SortedSet[BaseComparison]]
    if (PQ.isEmpty && H.size > 1)
      refineEmptyH()
  }

  def getTopComparison() : BaseComparison = {
    //val s = H(1.0f).asInstanceOf[mutable.SortedSet[BaseComparison]]
    val c = PQ.dequeue()
    //s.remove(c)
    c
  }

  var totSumSizes = 0L
  var totSumSizesCount = 0L
  var maxSumSize = 0L
  var countOne = 0L
  var countGreaterThanOne = 0L
  var performedComparisons = 0L
  var retrieved = 0L

  // Return the best comparisons
  override def getBestComparisons() = {
    if (!isEmpty()) {
      //assert(pq.head.sim >= pq.last.sim) //TODO remove
      retrieved += 1
      inserted -= 1
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
    println(s"[data] H.size = ${H.size}")
    val MAXRANGE = H.maxBefore(MAX_KEY) match {
      case Some(p) => p._1
      case None => -1.0f
    }
    println(s"[data] PQ size = ${PQ.size}")
    println(s"[data] max range = (${MAXRANGE}, KEY_MAX)")
    println(s"[data] thresh cut off ${tot/count.toFloat}")
    println(s"[data] Discarded ${discarded}")
    println(s"[data] Currently inserted ${inserted}")
    println(s"[data] Retrieved ${retrieved}")
    println(s"[update] Refine H overhead: ${refineOH/1000} s \n[update] Refine empty H overhead: ${refineEmptyOH/1000} s")
    println(s"[update] tMinAfter (s) = ${tMinAfter/1000}")
    println(s"[update] tUpdateSet (s) = ${tUpdateSet/1000}")
    println(s"[update] tUpdatePQ (s) = ${tUpdatePQ/1000}")
    println(s"[update] tComploop (s) = ${tCompLoop/1000} (NOTE tMinAfter, tUpdateSet and tUpdatePQ are included here)")
    println(s"[update] tPrunedComparisons (s) = ${tPruneComparisons/1000}")
    println(s"[update] tSum (s) = ${(refineOH+tMinAfter+tUpdateSet+tUpdatePQ)/1000}")
    println(s"[refineH] tDequeueAll (s) = ${tDequeueAll/1000}")
    println(s"[refineH] tMedian (s) = ${tMedian/1000}")
    println(s"[refineH] tPartition (s) = ${tPartition/1000}")
    println(s"[refineH] tNewPQ (s) = ${tNewPQ/1000}")
    println(s"[refineH] tInsertPQ (s) = ${tInsertPQ/1000}")
    println(s"[refineH] tNewH (s) = ${tNewH/1000}")
  }
}
