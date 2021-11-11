package com.parER.core.blocking

import com.parER.datastructure.BlockRanker
import org.scify.jedai.textmodels.TokenNGrams

class EntityTokenBlockerRefiner(size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f) extends TokenBlockerRefiner(size1, size2, ro, ff) {

  override def progressiveProcess(idx: Int, textModel: TokenNGrams) = {
    import scala.jdk.CollectionConverters._
    val textModelTokens = textModel.getSignatures.asScala.toList.map(_.trim).filter(0 < _.length)
    //if (textModel.getSignatures.asScala.toList.size == textModelTokens.size)
    //println("UUUUUUU")
    progressiveProcess(idx, textModel, textModelTokens)
  }

  // THIS METHOD SUPPORTS ONLY THE SCENARIO WHERE MODELSTORING IS FALSE
  override def progressiveProcess(idx: Int, textModel: TokenNGrams, keys: List[String]) = {
    val dId = textModel.getDatasetId
    // get blocks and apply block cutting and first step of block filtering
    val (associatedBlocks, associatedBlocksWithZeroSize) = getBlocks(idx, dId, keys)
    // get view of the hash map for later generation of comparisons
    //val view = invertedIndex.getModelIndexView(idx, textModel.getDatasetId, associatedBlocks.map(e => e._1))
    // build tuple and update inverted index data structure
    val blocks = (associatedBlocks map {case (k,v) => (k,v.toList)} ).toList
    val tuple = (idx, textModel, blocks)
    invertedIndex.update(idx, textModel, associatedBlocksWithZeroSize.map(e=> e._1) ++ associatedBlocks.map(e => e._1), modelStoring)
    tuple
  }

}
