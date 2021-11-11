package com.parER.akka.streams.prioritzer

import akka.stream.FlowShape
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler}
import com.parER.core.matching.SchemeJSMatcher
import com.parER.datastructure.{BaseComparison, Comparison}

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

class PPSGraphStageLogic(kMax: Int, updateFactor: Int, shape: FlowShape[List[BaseComparison], List[BaseComparison]]) extends GraphStageLogic(shape) {

  //println(s"kMax = ${kMax}; updateFactor = ${updateFactor}")

  val in = shape.in
  val out = shape.out
  val matcher = new SchemeJSMatcher

  // Ascending order (head is the lowest value)
  val pq = mutable.PriorityQueue()(Ordering.by[BaseComparison, Float](_.sim).reverse)

  val counterEntity = mutable.HashMap[Int, Float]().withDefaultValue(0.0f)

  val duplicationLikelihood = mutable.HashMap[Int, Float]().withDefaultValue(0.0f)

  var comparisonHashMap = mutable.HashMap[Int, mutable.PriorityQueue[BaseComparison]]()

  setHandler(in, new InHandler {

    override def onPush(): Unit = {
      val comparisons = matcher.execute(grab(in))
      val nComparisons = new ListBuffer[BaseComparison]()
      for (cmp <- comparisons) {
        if (pq.size < kMax)
          pq.enqueue(cmp)
        else if (pq.head.sim < cmp.sim ) {
          nComparisons += pq.dequeue()
          pq.enqueue(cmp)
        } else
          nComparisons += cmp
      }

      if (updateFactor > 0)
        update(nComparisons.result())

      push(out, pq.dequeueAll.toList)
    }

    override def onUpstreamFinish(): Unit = {
      //println("========== FILLING FINISHED =============")
      emitRemaining()
      completeStage()
    }
  })

  setHandler(out, new OutHandler {
    override def onPull(): Unit = {
      pull(in)
    }
  })

  def update(comparisons: List[BaseComparison]) = {
    for (cmp <- comparisons) {
      val e1 = cmp.e1
      duplicationLikelihood(e1) += cmp.sim
      counterEntity(e1) += 1
      if (!comparisonHashMap.contains(e1)) {
        comparisonHashMap(e1) = new mutable.PriorityQueue()(Ordering.by[BaseComparison, Float](_.sim).reverse)
        comparisonHashMap(e1).enqueue(cmp)
      } else if (comparisonHashMap(e1).size < kMax * updateFactor) {
        comparisonHashMap(e1).enqueue(cmp)
      } else {
        val last = comparisonHashMap(e1).head
        if (last.sim < cmp.sim ) {
          comparisonHashMap(e1).dequeue()
          comparisonHashMap(e1).enqueue(cmp)
        }
      }
    }
  }

  def emitRemaining() = {
    //Config.filling = true
    duplicationLikelihood.keys.foreach( e =>
      duplicationLikelihood(e) /= counterEntity(e)
    )
    val lm = immutable.ListMap(duplicationLikelihood.toSeq.sortWith(_._2 > _._2):_*)
    for ((e, _) <- lm) {
      val plist = new ListBuffer[BaseComparison]().addAll(comparisonHashMap(e)).sortWith(_.sim > _.sim)
      plist.grouped(kMax).foreach( lb => emit(out, lb.result))
    }
  }
}
