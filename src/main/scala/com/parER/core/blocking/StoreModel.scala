package com.parER.core.blocking

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, Comparison, LightWeightComparison}
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable.HashMap

class StoreModel(size1: Int = 16, size2: Int = 16) {

  println("Initial size1: " + size1)
  println("Initial size2: " + size2)

  val modelIndex = Array(
      HashMap.newBuilder[Int,TokenNGrams](2*size1, 0.75).result(),
      HashMap.newBuilder[Int,TokenNGrams](2*size2, 0.75).result())

  val ccer = Config.ccer

  def solveComparisons(comparisons: List[BaseComparison]) = {
    if (comparisons.size > 0) {
      val head = comparisons.head
      val cmps = if (head.e1Model != null) {
        val mi = getIndexForRetrieve(head.e1Model.getDatasetId)
        comparisons.map(c => new Comparison(c.e1, c.e1Model, c.e2, mi(c.e2)))
      } else {
        val mi = getIndexForRetrieve(head.e2Model.getDatasetId)
        comparisons.map(c => new Comparison(c.e1, mi(c.e1), c.e2, c.e2Model))
      }
      for (cmp <- cmps) {
        if (cmp.e1Model == null || cmp.e2Model == null) {
          println("ERRORISSIMO")
        }
      }
      cmps
    } else
      comparisons
  }

  def betterSolveMessageComparisons(comparisons: List[LightWeightComparison]): List[BaseComparison] = {
    if (comparisons.size > 0) {
      comparisons.map(c => {
          val mi0 = getIndexForRetrieve(0)
          val mi1 = getIndexForRetrieve(1)
          Comparison(c.e1, mi1(c.e1), c.e2, mi0(c.e2))
      })
    } else List[BaseComparison]()
  }

  def betterSolveComparisons(comparisons: List[BaseComparison]) = {
    if (comparisons.size > 0) {
      comparisons.map(c => {
        if (c.e1Model == null && c.e2Model == null) {
          val mi0 = getIndexForRetrieve(0)
          val mi1 = getIndexForRetrieve(1)
          Comparison(c.e1, mi1(c.e1), c.e2, mi0(c.e2))
        } else if (c.e1Model != null) {
          val mi = getIndexForRetrieve(c.e1Model.getDatasetId)
          Comparison(c.e1, c.e1Model, c.e2, mi(c.e2))
        } else {
          val mi = getIndexForRetrieve(c.e2Model.getDatasetId)
          Comparison(c.e1, mi(c.e1), c.e2, c.e2Model)
        }
      })
    } else comparisons
  }

  def solveComparisons(comparisons: List[BaseComparison], dId: Int) = {
    if (comparisons.size > 0) {
      val head = comparisons.head
      val cmps = if (head.e1Model != null) {
        val mi = getIndexForRetrieve(head.e1Model.getDatasetId)
        comparisons.map(c => new Comparison(c.e1, c.e1Model, c.e2, mi(c.e2)))
      } else {
        val mi = getIndexForRetrieve(head.e2Model.getDatasetId)
        comparisons.map(c => new Comparison(c.e1, mi(c.e1), c.e2, c.e2Model))
      }
      cmps
    } else
      comparisons
  }

  def solveUpdate(id: Int, model: TokenNGrams): Unit = {
    val mi = getIndexForUpdate(model.getDatasetId)
    mi.update(id, model)
  }

  private def getIndexForRetrieve(dId: Int) = {
    (dId, ccer) match {
      case (_, false) => modelIndex(0)
      case (0, true) => modelIndex(1)
      case (1, true) => modelIndex(0)
    }
  }

  private def getIndexForUpdate(dId: Int) = {
    (dId, ccer) match {
      case (_, false) => modelIndex(0)
      case (1, true) => modelIndex(1)
      case (0, true) => modelIndex(0)
    }
  }
}
