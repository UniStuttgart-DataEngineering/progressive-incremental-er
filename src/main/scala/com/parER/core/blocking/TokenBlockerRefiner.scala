package com.parER.core.blocking

import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable

class TokenBlockerRefiner(size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f) extends TokenBlocker {

  // Tokens to blacklist
  val criminalTokens = mutable.HashSet[String]()
  val maxBlockSize = Array(ro*size1, ro*size2)


  //println(s"TokenBlockerRefiner mbs1=${maxBlockSize(0)} ;; mbs2=${maxBlockSize(1)}")
  // Modify TokenBlocker.invertedIndex and build the list of comparisons out of it
  // Performs block cutting + block filtering + block purging if memory is saturated
  // TODO check correctness
  override def execute(idx: Int, textModel: TokenNGrams) = {
    import scala.jdk.CollectionConverters._
    val textModelTokens = textModel.getSignatures.asScala.toList
    execute(idx, textModel, textModelTokens)
  }

  override def execute(idx: Int, textModel: TokenNGrams, keys: List[String]) = {
    val dId = textModel.getDatasetId
    val (associatedBlocks, associatedBlocksWithZeroSize) = getBlocks(idx, dId, keys)

    // generate comparisons
    val comparisons = invertedIndex.generate(idx, textModel, associatedBlocks.map(e => e._1), modelStoring)
    invertedIndex.update(idx, textModel, associatedBlocksWithZeroSize.map(e=> e._1) ++ associatedBlocks.map(e => e._1), modelStoring)

    //if (comparisons.size > 0)
    //  comparisons.head.counters(0) = associatedBlocks.size

    comparisons
  }

  override def progressiveProcess(idx: Int, textModel: TokenNGrams) = {
    import scala.jdk.CollectionConverters._
    val textModelTokens = textModel.getSignatures.asScala.toList
    progressiveProcess(idx, textModel, textModelTokens)
  }

  override def progressiveProcess(idx: Int, textModel: TokenNGrams, keys: List[String]) = {
    val dId = textModel.getDatasetId
    // get blocks and apply block cutting and first step of block filtering
    val (associatedBlocks, associatedBlocksWithZeroSize) = getBlocks(idx, dId, keys)
    // get view of the hash map for later generation of comparisons
    //val view = invertedIndex.getModelIndexView(idx, textModel.getDatasetId, associatedBlocks.map(e => e._1))
    // build tuple and update inverted index data structure
    val blocks = associatedBlocks.map(e => (e._1, e._2.toList))
    val tuple = (idx, textModel, blocks)
    invertedIndex.update(idx, textModel, associatedBlocksWithZeroSize.map(e=> e._1) ++ associatedBlocks.map(e => e._1), modelStoring)
    tuple
  }

  override def process(idx: Int, textModel: TokenNGrams) = {
    import scala.jdk.CollectionConverters._
    val textModelTokens = textModel.getSignatures.asScala.toList
    process(idx, textModel, textModelTokens)
  }

  override def process(idx: Int, textModel: TokenNGrams, keys: List[String]) = {
    val dId = textModel.getDatasetId
    // get blocks and apply block cutting and first step of block filtering
    val (associatedBlocks, associatedBlocksWithZeroSize) = getBlocks(idx, dId, keys)
    // get view of the hash map for later generation of comparisons
    //val view = invertedIndex.getModelIndexView(idx, textModel.getDatasetId, associatedBlocks.map(e => e._1))
    // build tuple and update inverted index data structure
    val blocks = associatedBlocks.map(e => e._2.toList)
    val tuple = (idx, textModel, blocks)
    invertedIndex.update(idx, textModel, associatedBlocksWithZeroSize.map(e=> e._1) ++ associatedBlocks.map(e => e._1), modelStoring)
    tuple
  }


  def getBlocks(idx: Int, dId: Int, textModelTokens: List[String]) = {
    // Block cutting: cutted blocks are also purged
    //val allAssociatedBlocks = invertedIndex.associatedBlocks(idx, dId, textModelTokens, t => !criminalTokens.contains(t))
    //var (associatedBlocks, blocksToRemove) = allAssociatedBlocks.partition(_._2.size+1 < maxBlockSize(dId))
    var (associatedBlocks, associatedBlocksWithZeroSize, blocksToRemove) = invertedIndex.
      partitionedAssociatedBlocks(idx, dId, textModelTokens,
        !criminalTokens.contains(_), _.size+1 < maxBlockSize(dId))
    for ((t,b) <- blocksToRemove) {
      invertedIndex.remove(t)
      criminalTokens += t
    }

    // block filtering
    if (associatedBlocks.size > 0){
      val minSize = associatedBlocks.foldLeft(associatedBlocks(0)._2.size){ (min, e) => math.min(min, e._2.size) }
      associatedBlocks = associatedBlocks.filter{ case(t,b) => (b.size+1)*ff < minSize+1 } // TODO keep top-k ?
    }

    // block purging if memory is saturated
    // TODO
    (associatedBlocks, associatedBlocksWithZeroSize)
  }

  def getProgressiveBlocks(idx: Int, dId: Int, textModelTokens: List[String]) = {
    // Block cutting: cutted blocks are also purged
    //val allAssociatedBlocks = invertedIndex.associatedBlocks(idx, dId, textModelTokens, t => !criminalTokens.contains(t))
    //var (associatedBlocks, blocksToRemove) = allAssociatedBlocks.partition(_._2.size+1 < maxBlockSize(dId))
    var (associatedBlocks, associatedBlocksWithZeroSize, blocksToRemove) = invertedIndex.
      partitionedAssociatedBlocks(idx, dId, textModelTokens,
        !criminalTokens.contains(_), _.size+1 < maxBlockSize(dId))
    for ((t,b) <- blocksToRemove) {
      invertedIndex.remove(t)
      criminalTokens += t
    }

    // block filtering
    //if (associatedBlocks.size > 0){
    //  val minSize = associatedBlocks.foldLeft(associatedBlocks(0)._2.size){ (min, e) => math.min(min, e._2.size) }
    //  associatedBlocks = associatedBlocks.filter{ case(t,b) => (b.size+1)*ff < minSize+1 } // TODO keep top-k ?
    //}

    // block purging if memory is saturated
    // TODO
    //associatedBlocks.map(e => e._2.toList)
    (associatedBlocks map {case (k,v) => (k,v.toList)} )
  }
}
