package com.parER.core.blocking

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, Comparison, LightWeightComparison, MessageComparison, ProgressiveComparison}
import org.scify.jedai.textmodels.TokenNGrams

class CompGeneration {

  def process(idx: Int, textModel: TokenNGrams, blocks: List[List[Int]]) = {
    (idx, textModel, blocks.flatten)
  }

  def getComparisons(idx: Int, textModel: TokenNGrams, blocks: List[Int]) : List[BaseComparison] = {
    val comparisons = List.newBuilder[BaseComparison]
    (textModel.getDatasetId, Config.ccer) match {
      case (_, false) | (1, true) => for (i <- blocks) comparisons.addOne(Comparison(i, null, idx, textModel))
      case(0, true) => for (i <- blocks) comparisons.addOne(Comparison(idx, textModel, i, null))
    }
    comparisons.result()
  }

  def generateComparisons(idx: Int, textModel: TokenNGrams, blocks: List[List[Int]]) : List[BaseComparison] = {
    val comparisons = List.newBuilder[BaseComparison]
    (textModel.getDatasetId, Config.ccer) match {
      case (_, false) | (1, true) => for (block <- blocks; i <- block) comparisons.addOne(Comparison(i, null, idx, textModel))
      case(0, true) => for (block <- blocks; i <- block) comparisons.addOne(Comparison(idx, textModel, i, null))
    }
    comparisons.result()
  }

  def generateComparisonsWithoutTextModel(idx: Int, textModel: TokenNGrams, blocks: List[List[Int]]) : List[BaseComparison] = {
    val comparisons = List.newBuilder[BaseComparison]
    (textModel.getDatasetId, Config.ccer) match {
      case (_, false) | (1, true) => for (block <- blocks; i <- block) comparisons.addOne(Comparison(i, null, idx, null))
      case(0, true) => for (block <- blocks; i <- block) comparisons.addOne(Comparison(idx, null, i, null))
    }
    comparisons.result()
  }

  def generateMessageComparisons(idx: Int, textModel: TokenNGrams, blocks: List[List[Int]]) : List[LightWeightComparison] = {
    val comparisons = List.newBuilder[LightWeightComparison]
    (textModel.getDatasetId, Config.ccer) match {
      case (_, false) | (1, true) => for (block <- blocks; i <- block) comparisons.addOne(MessageComparison(i, idx))
      case(0, true) => for (block <- blocks; i <- block) comparisons.addOne(MessageComparison(idx, i))
    }
    comparisons.result()
  }

  def generateBlockMessageComparisons(blocks0: List[Int], blocks1: List[Int]) : List[LightWeightComparison] = {
    val comparisons = List.newBuilder[LightWeightComparison]
    for (i <- blocks0; j <- blocks1) comparisons.addOne(MessageComparison(i, j))
    comparisons.result()
  }

  def generateKeyToMessageComparisons(idx: Int, textModel: TokenNGrams, blocks: List[(String, List[Int])]) : Seq[(String, List[LightWeightComparison])] = {
    val keycomparisons = Seq.newBuilder[(String, List[LightWeightComparison])]
    for (s_b <- blocks) {
      val key = s_b._1
      val block = s_b._2
      val comparisons = List.newBuilder[LightWeightComparison]
      (textModel.getDatasetId, Config.ccer) match {
        case (_, false) | (1, true) => for (i <- block) comparisons.addOne(MessageComparison(i, idx))
        case (0, true) => for (i <- block) comparisons.addOne(MessageComparison(idx, i))
      }
      keycomparisons.addOne((key, comparisons.result()))
    }
    keycomparisons.result()
  }

  def progressiveComparisons(idx: Int, textModel: TokenNGrams, blocks: List[(String,List[Int])]) : List[BaseComparison] = {
    val comparisons = List.newBuilder[BaseComparison]
    (textModel.getDatasetId, Config.ccer) match {
      case (_, false) | (1, true) => for (block <- blocks; i <- block._2) comparisons.addOne(ProgressiveComparison(i, null, idx, textModel, 0.0f, block._1))
      case(0, true) => for (block <- blocks; i <- block._2) comparisons.addOne(ProgressiveComparison(idx, textModel, i, null, 0.0f, block._1))
    }
    comparisons.result()
  }

  // Put dId info
  def emptySeqProgressiveComparisons(items: Seq[(Int, TokenNGrams, List[(String, List[Int])])]) : List[BaseComparison] = {
    val comparisons = List.newBuilder[BaseComparison]
    for (s <- items) {
      val idx = s._1
      val textModel = s._2
      val blocks = s._3
      (textModel.getDatasetId, Config.ccer) match {
        case (_, false) | (1, true) => for (block <- blocks; i <- block._2) comparisons.addOne(ProgressiveComparison(i, null, idx, null, 0.0f, block._1, 1))
        case(0, true) => for (block <- blocks; i <- block._2) comparisons.addOne(ProgressiveComparison(idx, null, i, null, 0.0f, block._1, 0))
      }
    }
    comparisons.result()
  }

  def emptySeqProgressiveComparisonsToPairs(items: Seq[(Int, TokenNGrams, List[(String, List[Int])])]) : Seq[(Int, List[BaseComparison])] = {
    items.map(s => {
      val idx = s._1
      val textModel = s._2
      val blocks = s._3
      val comparisons = List.newBuilder[BaseComparison]
      (textModel.getDatasetId, Config.ccer) match {
        case (_, false) | (1, true) => for (block <- blocks; i <- block._2) comparisons.addOne(ProgressiveComparison(i, null, idx, null, 0.0f, block._1, 1))
        case(0, true) => for (block <- blocks; i <- block._2) comparisons.addOne(ProgressiveComparison(idx, null, i, null, 0.0f, block._1, 0))
      }
      (textModel.getDatasetId, comparisons.result())
    })
  }

  def progressiveBlockComparisons(idx: Int, textModel: TokenNGrams, block: List[Int]) = {
    val comparisons = List.newBuilder[BaseComparison]
    (textModel.getDatasetId, Config.ccer) match {
      case (_, false) | (1, true) => for (i <- block) comparisons.addOne(Comparison(i, null, idx, textModel, 0.0f))
      case(0, true) => for (i <- block) comparisons.addOne(Comparison(idx, textModel, i, null))
    }
    comparisons.result()
  }

  //def blockComparisons(idx: Int, textModel: TokenNGrams, blocks: List[(String,List[Int])]) : List[BaseComparison] = {
  def blockComparisons(dId: Int, profiles: List[(Int, TokenNGrams)], block: (String, List[Int])) = {
    val comparisons = List.newBuilder[BaseComparison]
    (dId, Config.ccer) match {
      case (_, false) | (1, true) => for (i <- block._2; (idx, textModel) <- profiles) comparisons.addOne(ProgressiveComparison(i, null, idx, textModel, 0.0f, block._1))
      case(0, true) => for (i <- block._2; (idx, textModel) <- profiles) comparisons.addOne(ProgressiveComparison(idx, textModel, i, null, 0.0f, block._1))
    }
    comparisons.result()
  }

  def blockComparisonsEmpty(dId: Int, profiles: List[Int], block: (String, List[Int])) = {
    val comparisons = List.newBuilder[BaseComparison]
    (dId, Config.ccer) match {
      case (_, false) | (1, true) => for (i <- block._2; idx <- profiles) comparisons.addOne(ProgressiveComparison(i, null, idx, null, 0.0f, block._1))
      case(0, true) => for (i <- block._2; idx <- profiles) comparisons.addOne(ProgressiveComparison(idx, null, i, null, 0.0f, block._1))
    }
    comparisons.result()
  }

  def blockMessageComparisonsEmpty(dId: Int, profiles: List[Int], block: (String, List[Int])) = {
    val comparisons = List.newBuilder[LightWeightComparison]
    (dId, Config.ccer) match {
      case (_, false) | (1, true) => for (i <- block._2; idx <- profiles) comparisons.addOne(MessageComparison(i, idx))
      case(0, true) => for (i <- block._2; idx <- profiles) comparisons.addOne(MessageComparison(idx, i))
    }
    comparisons.result()
  }
}
