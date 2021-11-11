package com.parER.core.prioritizing

class PQKPrioritizer(K: Int) extends PQPrioritizer {

  // Return the best comparisons
  override def getBestComparisons() = {
    if (!pq.isEmpty) {
      List.fill(math.min(K, pq.size))(getTopComparison())
    } else
      List()
  }

}
