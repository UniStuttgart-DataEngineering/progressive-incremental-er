package com.parER.core.prioritizing

import com.parER.core.matching.SchemeJSMatcher
import com.parER.datastructure.{BaseComparison, Comparison}

import scala.collection.mutable

class PpsPrioritizer(val kMax: Int) extends Prioritizer {

  val duplicationLikelihood = mutable.HashMap[Int, Float]().withDefaultValue(0.0f)
  //val distinctNeighbors = mutable.HashMap[Int, Int]().withDefaultValue(0)
  //val pq = mutable.SynchronizedPriorityQueue()(Ordering.by[BaseComparison, Float](_.sim))
  val pq = mutable.PriorityQueue()(Ordering.by[BaseComparison, Float](_.sim))
  val matcher = new SchemeJSMatcher
  var totSim = 0.0f

  override def execute(comparisons: List[BaseComparison]) = {
    if (comparisons.size != 0) {
      for (cmp <- comparisons) {
        matcher.execute(cmp)
      }
    }
  }

  // TODO this ignore the Float threshold, search for a better solution
  override def execute(comparisons: List[BaseComparison], threshold: Float) = {
    if (comparisons.size == 0)
      comparisons
    else {
      execute(comparisons)
      get(kMax)
    }
  }

  override def get() = pq.dequeueAll.toList

  override def get(k: Int) = {
    val comparisons = List.newBuilder[BaseComparison]
    for (i <- 0 until k if !pq.isEmpty)
      comparisons.addOne(pq.dequeue())
    comparisons.result()
  }

  def update(comparisons: List[BaseComparison]) = {

  }

  def getOthers() = {
    pq.toList
  }

  def reset() = {
    pq.clear()
  }

  def get(predicate: BaseComparison => Boolean) = {
    null
  }

  override def hasComparisons(): Boolean = !pq.isEmpty
}
