package com.parER.core.clustering

import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.datamodel.EquivalenceCluster

trait Clustering {
  def getDuplicates(comparisons: List[BaseComparison]): Array[EquivalenceCluster]
}
