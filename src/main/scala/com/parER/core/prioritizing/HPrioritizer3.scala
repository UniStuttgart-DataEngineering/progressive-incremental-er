package com.parER.core.prioritizing

import com.parER.datastructure.BaseComparison

import scala.collection.mutable

/**
 *
 * Comparison centric approach for prioritizer using a Tree Map. As illustrated in the paper.
 *
 */

class HPrioritizer3 extends Prioritizing {

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

  // TODO use plain priority queue for 1.0f insertions.

  def getSortedSet() = mutable.SortedSet[BaseComparison]()(Ordering.by[BaseComparison, (Float, (Int, Int))](x => (x.sim, (x.e1, x.e2))).reverse)
  def refineEmptyH() = {
    //println("refineEmpty...")
    val t0 = System.currentTimeMillis()
    val (k, v) = H.maxBefore(1.0f) match { case Some(p) => p }
    //println(s"filling PQ with ${v.size} comparisons")
    PQ ++= v
    H.remove(k)
    var delta = (System.currentTimeMillis()-t0)
    refineEmptyOH += delta
    //println("End RefineEmptyH... " + delta + " ms")
  }

  var tDequeueAll, tMedian, tPartition, tNewPQ, tNewH, tInsertPQ = 0L

  def refineH() = {

    var t0 = System.currentTimeMillis()
    val T0 = t0
    // TODO assert first is higher or equal than last
    //assert(l(0).sim >= l.last.sim)
//    println("Refine H... " + H.size)
//    println("H size: " + H.size)
//    println("Distribution: " + H.map(_._2.size).toList)
//    println("PQ size " + PQ.size)
//    println("Tot number of comparisons: " + H.map(_._2).foldLeft(0.0f)((a,b) => a + b.size))

    val l = PQ.dequeueAll.toList
    tDequeueAll += (System.currentTimeMillis()-t0)

    t0 = System.currentTimeMillis()
    var m = l(Math.ceil(l.length/2).toInt).sim
    if (m == l(0).sim) {
      m = l.foldLeft(0.0f)((x,y)=>x+y.sim) / l.size
    }
    tMedian += (System.currentTimeMillis()-t0)

    if (m != l(0).sim) {

      t0 = System.currentTimeMillis()
      val p = l.partition(x => x.sim > m)
      tPartition += (System.currentTimeMillis()-t0)
      //println("Refine H... " + H.size + " and " + l.size)
      //println(l(0).sim + "----" + l.last.sim + "----" + m + s"=====(${p._1.size}), ${p._2.size}")

      // TODO what if similar values?
      //if (p._1.last.sim == p._2(0).sim)
      //  assert(p._1.last.sim != p._2(0).sim)

      //H.remove(1.0f)
      //H(1.0f) = getSortedSet() ++ p._1
      t0 = System.currentTimeMillis()
      PQ ++= p._1
      tNewPQ += (System.currentTimeMillis()-t0)

      t0 = System.currentTimeMillis()
      H(m) = new mutable.HashSet[BaseComparison]() ++ p._2
      tNewH += (System.currentTimeMillis()-t0)

    } else {
      t0 = System.currentTimeMillis()
      PQ ++= l
      tInsertPQ += (System.currentTimeMillis()-t0)
    }

    //println(s"Splited at ${m}")
    //println("End Refine H... " + (System.currentTimeMillis()-t0) + " ms")
    refineOH += (System.currentTimeMillis()-T0)
  }

  val maxComparisons = 1e8
  val maxSize = 1e6
  val H = new mutable.TreeMap[Float, mutable.Set[BaseComparison]]()
  val PQ = mutable.PriorityQueue()(Ordering.by[BaseComparison, Float](_.sim))
  H(1.0f) = getSortedSet()

  var count = 0L
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

  def updatePQ(c: BaseComparison) = {
    val t = System.currentTimeMillis()
    PQ.enqueue(c)
    tUpdatePQ += (System.currentTimeMillis()-t)
  }

  var tot = 0.0f
  override def update(cmps: List[BaseComparison]) = {
    for (c <- cmps) {

      tot += c.sim
      count += 1

      // TODO assumption that all the cmps have sim <= 1
      if (c.sim > 1.0f) {
        println("AO CE STA QUALCOSA CHE NON VA EDDAJEEEEEEEEE")
        assert(c.sim <= 1.0f)
      }

      var t = System.currentTimeMillis()
      val x = H.minAfter(c.sim)
      tMinAfter += (System.currentTimeMillis()-t)

      x match {
        case None => println("H: ERRORE")
        case Some(p) => {
          val k = p._1
          val s = p._2
          if (k != 1.0f) {
            if (c.sim > (tot/count.toFloat)) {
              updateSet(s,c) //s += c
            } else {
              discarded += 1
            }
          } else
            updatePQ(c) //PQ.enqueue(c)
        }
      }
    }

    if (maxSize < PQ.size) {
      //println(s"RefineH: PQ.size = ${PQ.size} ; From Beginning ${(System.currentTimeMillis()-TIME0)/1000} s")
      refineH()
    }

    val t = System.currentTimeMillis()
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
    tPruneComparisons += (System.currentTimeMillis()-t)

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
  def overhead() = {
    println(s"[data] thresh cut off ${tot/count.toFloat}")
    println(s"[data] Discarded ${discarded}")
    println(s"[update] Refine H overhead: ${refineOH/1000} s \n[update] Refine empty H overhead: ${refineEmptyOH/1000} s")
    println(s"[update] tMinAfter (s) = ${tMinAfter/1000}")
    println(s"[update] tUpdateSet (s) = ${tUpdateSet/1000}")
    println(s"[update] tUpdatePQ (s) = ${tUpdatePQ/1000}")
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
