package com.parER.core.clustering

import java.util
import java.util.PriorityQueue

import com.esotericsoftware.minlog.Log
import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, Comparison}
import gnu.trove.set.hash.TIntHashSet
import org.scify.jedai.datamodel.{EquivalenceCluster, SimilarityEdge, SimilarityPairs}
import org.scify.jedai.entityclustering.{AbstractEntityClustering, UniqueMappingClustering}
import org.scify.jedai.utilities.comparators.DecSimilarityEdgeComparator
import org.scify.jedai.utilities.graph.UndirectedGraph

class ScalaUniqueMappingClustering(val t: Float) extends AbstractEntityClustering(t) with Clustering {

  val matchedIds = new TIntHashSet //the ids of entities that have been already matched

  override def getDuplicates(comparisons: List[BaseComparison]): Array[EquivalenceCluster] = {
    if (comparisons.size == 0) return new Array[EquivalenceCluster](0)

    initializeData(comparisons)
    if (!isCleanCleanER) return null //the method is only applicable to Clean-Clean ER

    val SEqueue: util.Queue[SimilarityEdge] = new PriorityQueue[SimilarityEdge](comparisons.size, new DecSimilarityEdgeComparator)

    for (b <- comparisons) {
      if (threshold < b.sim) SEqueue.add(new SimilarityEdge(b.e1, b.e2 + datasetLimit, b.sim))
    }

    Log.info("Retained comparisons\t:\t" + SEqueue.size)

    while ( { !SEqueue.isEmpty }) {
      val se: SimilarityEdge = SEqueue.remove
      val e1: Int = se.getModel1Pos
      val e2: Int = se.getModel2Pos
      //skip already matched entities (unique mapping contraint for clean-clean ER)
      if (matchedIds.contains(e1) || matchedIds.contains(e2)) {}
      else {
        similarityGraph.addEdge(e1, e2)
        matchedIds.add(e1)
        matchedIds.add(e2)
      }
    }

    return getConnectedComponents
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
    a.foldLeft((a.head.e1, a.head.e2)) { case ((max1, max2), e) => (math.max(max1, e.e1), math.max(max2, e.e2))}
  }

  override def getDuplicates(simPairs: SimilarityPairs): Array[EquivalenceCluster] = null

  override def getMethodInfo: String = ???

  override def getMethodName: String = ???
}
