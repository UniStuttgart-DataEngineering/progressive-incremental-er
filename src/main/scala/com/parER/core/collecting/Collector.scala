package com.parER.core.collecting

import com.parER.core.Config
import com.parER.core.clustering.{ScalaConnectedComponentsClustering, ScalaUniqueMappingClustering}
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.datamodel.EquivalenceCluster

import scala.collection.mutable.ListBuffer

class Collector(val threshold: Float = 0.5f) {

  var buffer : ListBuffer[BaseComparison] = new ListBuffer()
  var curr_recall: Float = 0.0f
  val ccer = Config.ccer

  // TODO improve, use a static ScalaConnectedComponentsClustering?
  def execute(comparisons: List[BaseComparison], accumulate: Boolean = true) = {
    if (accumulate) buffer ++= comparisons
    // here computation is done after each entity "chunk"
    // TODO perform computation in intervals e.g. every 1 minute?
    val ec = if (ccer) new ScalaUniqueMappingClustering(threshold) else new ScalaConnectedComponentsClustering(threshold)
    var clusters = null : Array[EquivalenceCluster]
    if (!buffer.isEmpty && accumulate)
      clusters = ec.getDuplicates(buffer.toList)
    else if (!accumulate)
      clusters = ec.getDuplicates(comparisons)
    clusters
  }

}
