package com.parER.core.blocking

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable.ListBuffer

class BlockGhosting(val ff: Float = 0.05f) {

  def process(id: Int, model: TokenNGrams, blocks: List[List[Int]]) = {
    (id, model, getBlocks(blocks))
  }

  private def getBlocks(blocks: List[List[Int]]) = {
    if (blocks.size > 0){
      val minSize = blocks.foldLeft(blocks(0).size){ (min, e) => math.min(min, e.size) }
      blocks.filter(b => (b.size+1)*ff < minSize+1)
    } else
      blocks
  }

  def progressiveProcess(id: Int, model: TokenNGrams, blocks: List[(String,List[Int])]) = {
    (id, model, progressiveGetBlocks(blocks))
  }

  private def progressiveGetBlocks(blocks: List[(String,List[Int])]) = {
    if (blocks.size > 0){
      val minSize = blocks.foldLeft(blocks(0)._2.size){ (min, e) => math.min(min, e._2.size) }
      blocks.filter(b => (b._2.size+1)*ff < minSize+1)
    } else
      blocks
  }

  private def generateComparisons(idx: Int, textModel: TokenNGrams, blocks: ListBuffer[List[Int]]) = {
    val comparisons = List.newBuilder[BaseComparison]
    (textModel.getDatasetId, Config.ccer) match {
      case (_, false) | (1, true) => for (block <- blocks; i <- block) comparisons.addOne(new Comparison(i, null, idx, textModel))
      case(0, true) => for (block <- blocks; i <- block) comparisons.addOne(new Comparison(idx, textModel, i, null))
    }
    comparisons.result()
  }

}
