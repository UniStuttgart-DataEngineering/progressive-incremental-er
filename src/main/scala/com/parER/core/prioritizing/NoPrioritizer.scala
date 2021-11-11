package com.parER.core.prioritizing
import com.parER.datastructure.{BaseComparison, Comparison}

import scala.collection.mutable

class NoPrioritizer extends Prioritizer {

  var buffer = mutable.Queue[BaseComparison]()
  //val bfMatcher = new BFMatcher(1024, 1)

  override def execute(comparisons: List[BaseComparison]): Unit = {
    buffer ++= comparisons
  }

  override def execute(comparisons: List[BaseComparison], threshold: Float): List[BaseComparison] = {
    comparisons
  }

  override def get(): List[BaseComparison] = buffer.dequeueAll(_ => true).toList

  override def get(k: Int) = buffer.dequeueAll(_ => true).toList // should not be called

  override def hasComparisons(): Boolean = !buffer.isEmpty
}
