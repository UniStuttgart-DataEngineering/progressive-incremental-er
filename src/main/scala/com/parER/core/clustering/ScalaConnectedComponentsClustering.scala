package com.parER.core.clustering

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.datamodel.EquivalenceCluster
import org.scify.jedai.entityclustering.ConnectedComponentsClustering
import org.scify.jedai.utilities.graph.UndirectedGraph

class ScalaConnectedComponentsClustering(val t: Float) extends ConnectedComponentsClustering(t) with Clustering {

  override def getDuplicates(comparisons: List[BaseComparison]): Array[EquivalenceCluster] = {
    initializeData(comparisons)
    // add an edge for every pair of entities with a weight higher than the threshold
    for (cmp <- comparisons if threshold < cmp.sim) {
      similarityGraph.addEdge(cmp.e1, cmp.e2 + datasetLimit)
    }
    getConnectedComponents()
  }

  protected def initializeData(comparisons: List[BaseComparison]): Unit = {
    isCleanCleanER = Config.ccer
    val (maxEntity1, maxEntity2) = getMaxes(comparisons)
    if (isCleanCleanER) {
      datasetLimit = maxEntity1 + 1
      noOfEntities = maxEntity1 + maxEntity2 + 2
    } else {
      datasetLimit = 0
      noOfEntities = Math.max(maxEntity1, maxEntity2) + 1
    }
    similarityGraph = new UndirectedGraph(noOfEntities)
  }

  protected def getMaxes(a: List[BaseComparison]) : (Int, Int) = {
    if (a.isEmpty)
      return (Integer.MIN_VALUE, Integer.MIN_VALUE)
    a.foldLeft((a(0).e1, a(0).e2)) { case ((max1, max2), e) => (math.max(max1, e.e1), math.max(max2, e.e2))}
  }
}
