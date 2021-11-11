package com.parER.core.entityscheduling

import java.util.Comparator

import com.google.common.collect.MinMaxPriorityQueue
import com.parER.akka.streams.utils.EntityComparator
import com.parER.core.Config
import com.parER.datastructure.{LightWeightComparison, MessageComparison}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class EntityPrioritizer {

  val ccer = Config.ccer

  // Associates (e, dId) to an EntityContainer
  val index = new mutable.HashMap[(Int, Short), EntityContainer]()

  // Entities in form (e, dId, rank) in a priority queue
  val entityQueue = mutable.PriorityQueue()(Ordering.by[(Int, Short, Float), Float](_._3))

  val dmaxComparisons = (100.0 * Config.nduplicates).toInt

//  def comparator = new Comparator[LightWeightComparison] {
//    override def compare(o1: LightWeightComparison, o2: LightWeightComparison): Int = {
//      -java.lang.Float.compare(o1.sim, o2.sim)
//    }
//  }

  // Secondary pq
  val dpq : MinMaxPriorityQueue[LightWeightComparison] = MinMaxPriorityQueue
    .orderedBy(EntityComparator)
    .maximumSize(dmaxComparisons)
    .create()

  var UpdateTime = 0L

  // Global pruning
  var tot = 0.0f
  var count = 0L

  var pruning = true

  // Debug...
  var countEmpty = 0

  // Times
  var t0, tGetComparisons = 0L
  var countComparisons = 0
  var tGetTopComparison, tIsEmpty = 0L

  // Boolean flags
  var indexUpdated = true
  var indexEmpty = false

  // Pruning flag
  def setPruning(value: Boolean) = {
    pruning = value
    EntityContainer.pruning = value
  }

  private def getTopComparison() : LightWeightComparison = {
    if (ccer) {
      val tBegin = System.currentTimeMillis()
      val (e1, dId, rank1) = entityQueue.dequeue()
      val e1Container = index((e1, dId))
      if (e1Container.size > 0) {
        val (e2, rank2) = e1Container.pop()
        val m = if (dId == 0)
          MessageComparison(e1, e2)
        else
          MessageComparison(e2, e1)
        tGetTopComparison += (System.currentTimeMillis() - tBegin)
        m
      } // TODO decomment if dpq is not present
      //    else if (!dpq.isEmpty)
      //      dpq.removeFirst()
      else
      null
    } else {
      // FOR DIRTY ER
      val tBegin = System.currentTimeMillis()
      val (e1, dId, rank1) = entityQueue.dequeue()
      val e1Container = index((e1, dId))
      if (e1Container.size > 0) {
        val (e2, rank2) = e1Container.pop()
        val m = if (e1 < e2)
          MessageComparison(e1, e2)
        else
          MessageComparison(e2, e1)
        tGetTopComparison += (System.currentTimeMillis() - tBegin)
        m
      } // TODO decomment if dpq is not present
      //    else if (!dpq.isEmpty)
      //      dpq.removeFirst()
      else
      null
    }
  }

  def getBestComparisons() = {
    if (entityQueue.isEmpty)
      for (((e1, dId), ec) <- index) {
        val (e2, rank) = ec.topToStack()
        entityQueue.addOne((e1, dId, rank))
      }

    if (!entityQueue.isEmpty) {
      List(getTopComparison())
    } else
      List()
  }

  def isEmpty() : Boolean = {
    val tBegin = System.currentTimeMillis()
    if (entityQueue.isEmpty) {
      countEmpty += 1
      //if (!Config.upstreamTerminated) {
        for (((e1, dId), ec) <- index) {
          val p = ec.topToStack()
          if (p != null) {
            entityQueue.addOne((e1, dId, p._2))
          }
        }
      //}
    }
    tIsEmpty += (System.currentTimeMillis() - tBegin)
    entityQueue.isEmpty
  }

  def getTopComparisons(K: Int) = {
    t0 = System.currentTimeMillis()
    val comparisons = ListBuffer[LightWeightComparison]()
    // TODO moved if (!isEmpty before) for experiments
    if ((!indexEmpty || indexUpdated) && !isEmpty()) {
      indexEmpty = false
      for (i <- 0 until K if entityQueue.size > 0) {
        //if (!isEmpty()) {
          val tc = getTopComparison()
          if (tc != null)
            comparisons += tc
        //}
      }
    } else {
      // if called again isEmpty() will return the same answer.
      // answer may change after call of update.
      // --> execute if-then ONLY if previous isEmpty was false or index has been updated.
      indexEmpty = true
      indexUpdated = false
    }

    // TODO decomment this if dpq is not present
    if (comparisons.size < K) {
      val diff = K - comparisons.size
      for (i <- 0 until diff) {
        if (!dpq.isEmpty)
          comparisons += dpq.removeFirst()
      }
    }
    countComparisons  +=  comparisons.size
    tGetComparisons   +=  (System.currentTimeMillis() - t0)
    comparisons.result()
  }

//  def update(comparisons: List[LightWeightComparison]) = {
//    //if (!pruning) println("Updating .... " + comparisons.size)
//
//    val w = comparisons.foldLeft(0.0f)( (v, c) => v + c.sim).toFloat / comparisons.size
//    val splittedCmps = comparisons.partition(x => x.sim >= w)
//
//    for (c <- splittedCmps._1) {
//      val sim = c.sim.toFloat
//      val k1 = (c.e1, 0.toShort)
//      val k2 = (c.e2, 1.toShort)
//      if (!index.contains(k1))
//        index.addOne(k1, new EntityContainer(c.e1, 0))
//      if (!index.contains(k2))
//        index.addOne(k2, new EntityContainer(c.e2, 1))
//      val ec1 = index(k1)
//      val ec2 = index(k2)
//      if (sim > ec1.rank) {
//        entityQueue.addOne((c.e1, 0, sim))
//        ec1.push(c.e2, sim)
//      } else if (sim > ec2.rank) {
//        entityQueue.addOne((c.e2, 1, sim))
//        ec2.push(c.e1, sim)
//      } else {
//        if (ec1.size < ec2.size) {
//          ec1.enqueue(c.e2, sim)
//        } else {
//          ec2.enqueue(c.e1, sim)
//        }
//      }
//    }
//
//    if (splittedCmps._2.size + dpq.size > dmaxComparisons) {
//      for (c <- splittedCmps._2)
//        dpq.add(c)
//    }
//  }

  // OLD VERSION
  def update(comparisons: List[LightWeightComparison]) = {
    val t0 = System.currentTimeMillis()
    indexUpdated = true
    val w = if (pruning)
      comparisons.foldLeft(0.0f)( (v, c) => v + c.sim) / comparisons.size
      else 0.0f
    //if (!pruning) println("Updating .... " + comparisons.size)
    for (c <- comparisons.filter(_.sim >= w)) {
    //for (c <- comparisons) {
      val sim = c.sim.toFloat
      val did1 = if (ccer) 1.toShort else 0.toShort
      val k1 = (c.e1, 0.toShort)
      val k2 = (c.e2, did1)
      if (pruning) {
        count += 1
        tot += sim
      }
      if (!index.contains(k1))
        index.addOne(k1, new EntityContainer(c.e1, 0))
      if (!index.contains(k2))
        index.addOne(k2, new EntityContainer(c.e2, did1))
      val ec1 = index(k1)
      val ec2 = index(k2)
      if (sim > ec1.rank) {
        entityQueue.addOne((c.e1, 0, sim))
        ec1.push(c.e2, sim)
      } else if (sim > ec2.rank) {
        entityQueue.addOne((c.e2, did1, sim))
        ec2.push(c.e1, sim)
      } else {
        // TODO check if it works...
        //val (s, ms) = index.values.foldLeft((0.0,0.0))((x,y) => (x._1+y.size,x._2+y.maxSize))
        if (!pruning || sim > (tot/count)) {
        //if (!pruning || s/ms < 0.1 || sim > (tot/count)) {
          if (ec1.size < ec2.size) {
            ec1.enqueue(c.e2, sim)
          } else {
            ec2.enqueue(c.e1, sim)
          }
        }
        // TODO decomment if dpq does not exist
        else {
          dpq.add(c)
        }
      }
    }

    UpdateTime += (System.currentTimeMillis()-t0)

  }

  def almostEmpty(K: Int) = {
    entityQueue.size < K
  }

  def overhead() = {
    println("UpdateTime1 (pq) in seconds: " + UpdateTime/1000)
    println("UpdateTime1 (pq) in minutes: " + UpdateTime.toDouble/(1000*60))
    println("EntityQueue size: " + entityQueue.size)
    println("EntityQueue load: " + (entityQueue.size.toDouble / Config.nduplicates))

    val (s, ms) = index.values.foldLeft((0.0,0.0))((x,y) => (x._1+y.size,x._2+y.maxSize))
    println(s"Comparisons in index: ${s}/${ms}")
    println(s"Index load factor: ${s/ms}")
    //////////////////
//    println("mmmm")
//    println("Queue size: " + entityQueue.size)
//    println("Count empty: " + countEmpty)
    println("Time top comparisons (min): " + tGetComparisons.toDouble/(1000*60))
    println("Time isEmpty (min): " + tIsEmpty.toDouble/(1000*60))

    println("Number of comparisons: " + countComparisons)

    val inserted = index.values.foldLeft(0L)(_ + _.inserted)
    println(s"Number of comparisons inserted in index: ${inserted}")
    println(s"Size of dpq: ${dpq.size()}")
    //    println(s"Comparisons x second: ${countComparisons*1000/tGetComparisons}")
//    println(s"tGetTopComparison = $tGetTopComparison ms")
//    println(s"tIsEmpty = $tIsEmpty ms")
//    countComparisons = 0
    //tGetComparisons = 0
//    tGetTopComparison = 0
//    tIsEmpty = 0
    ///////////////
  }

}
