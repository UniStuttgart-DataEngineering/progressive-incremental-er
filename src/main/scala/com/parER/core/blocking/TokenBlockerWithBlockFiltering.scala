package com.parER.core.blocking

import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable

class TokenBlockerWithBlockFiltering(size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.8f) extends TokenBlocker {

  // Tokens to blacklist
  val criminalTokens = mutable.HashSet[String]()
  val maxBlockSize = Array(ro*size1, ro*size2)

  // Modify TokenBlocker.invertedIndex and build the list of comparisons out of it
  // Performs block cutting + block filtering + block purging if memory is saturated
  // TODO check correctness
  override def execute(idx: Int, textModel: TokenNGrams) = {
    import scala.jdk.CollectionConverters._
    val textModelTokens = textModel.getSignatures.asScala.toList
    val dId = textModel.getDatasetId
    val (associatedBlocks, associatedBlocksWithZeroSize) = getBlocks(idx, dId, textModelTokens)

    // generate comparisons
    val comparisons = invertedIndex.generate(idx, textModel, associatedBlocks.map(e => e._1))
    invertedIndex.update(idx, textModel, associatedBlocksWithZeroSize.map(e=> e._1) ++ associatedBlocks.map(e => e._1))

    //if (comparisons.size > 0)
    //  comparisons.head.counters(0) = associatedBlocks.size
    
    comparisons
  }

  def getBlocks(idx: Int, dId: Int, textModelTokens: List[String]) = {
    var (associatedBlocks, associatedBlocksWithZeroSize, blocksToRemove) = invertedIndex.
      partitionedAssociatedBlocks(idx, dId, textModelTokens,
        !criminalTokens.contains(_), _.size+1 < maxBlockSize(dId))
    for ((t,b) <- blocksToRemove) {
      invertedIndex.remove(t)
      criminalTokens += t
    }

    // block filtering: here it retains the ff*blocks.size of most important blocks
    if (associatedBlocks.size > 0){
      val index = (associatedBlocks.size.toFloat * ff).ceil
      associatedBlocks.sortWith( _._2.size < _._2.size )
      associatedBlocks = associatedBlocks.splitAt(index.toInt)._1
    }

    // block purging if memory is saturated
    // TODO
    (associatedBlocks, associatedBlocksWithZeroSize)
  }
}
