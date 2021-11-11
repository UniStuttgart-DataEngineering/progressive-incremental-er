package com.parER.datastructure

import java.util.concurrent.ConcurrentHashMap

import com.parER.core.Config
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.jdk.CollectionConverters._

class ConcurrentBlockingService(size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f) {

  val maxBlockSize = Array(ro*size1, ro*size2)

  def execute(idx: Int, textModel: TokenNGrams) = {
    println(idx)
    val textModelTokens = textModel.getSignatures.asScala.toList
    val dId = textModel.getDatasetId
    val (associatedBlocks, associatedBlocksWithZeroSize) = getBlocks(idx, dId, textModelTokens)
    val comparisons = generate(idx, textModel, associatedBlocks.map(e => e._1))
    update(idx, textModel, associatedBlocksWithZeroSize.map(e=> e._1) ++ associatedBlocks.map(e => e._1))

    comparisons
  }

  def getBlocks(idx: Int, dId: Int, textModelTokens: List[String]) = {
    // get blocks and block cutting
    var (associatedBlocks, associatedBlocksWithZeroSize) = ConcurrentBlockingService.getAssociatedBlocks(idx, dId, textModelTokens, maxBlockSize)

    // block filtering
    if (associatedBlocks.size > 0){
      val minSize = associatedBlocks.foldLeft(associatedBlocks(0)._2.size){ (min, e) => math.min(min, e._2.size) }
      associatedBlocks = associatedBlocks.filter{ case(t,b) => (b.size+1)*ff < minSize+1 } // TODO keep top-k ?
    }

    (associatedBlocks, associatedBlocksWithZeroSize)
  }

  def update(idx: Int, textModel: TokenNGrams, textModelTokens: List[String]): Unit = {
    val (mi, ii) = ConcurrentBlockingService.getIndexesForUpdate(textModel.getDatasetId)
    for (token <- textModelTokens) {
      ii(token) += idx
    }
    mi(idx) = textModel
  }

  // TODO guarantee correct order: e1(0) - e2(1) is a different BaseComparison than e2(0) - e1(1)
  def generate(idx: Int, textModel: TokenNGrams, textModelTokens: List[String]) = {
    val dId = textModel.getDatasetId
    val comparisons = List.newBuilder[BaseComparison]
    val (mi, ii) = ConcurrentBlockingService.getIndexesForRetrieve(dId)
    (dId, ConcurrentBlockingService.ccer) match {
      case (_, false) | (1, true) => for (token <- textModelTokens; i <- ii(token)) comparisons.addOne(new Comparison(i, mi(i), idx, textModel))
      case(0, true) => for (token <- textModelTokens; i <- ii(token)) comparisons.addOne(new Comparison(idx, textModel, i, mi(i)))
    }
    comparisons.result()
  }
}

object ConcurrentBlockingService {
  import scala.jdk.CollectionConverters._

  val invertedIndex = Array(
    new HashMap[String, ListBuffer[Int]]() { override def apply(key: String) = super.getOrElseUpdate(key, ListBuffer())},
    new HashMap[String, ListBuffer[Int]]() { override def apply(key: String) = super.getOrElseUpdate(key, ListBuffer())})
  val modelIndex = Array( HashMap[Int, TokenNGrams](), HashMap[Int, TokenNGrams]())
  val ccer = Config.ccer

  //val invertedIndex = Array( new ConcurrentHashMap[String, ListBuffer[Int]]().asScala , new ConcurrentHashMap[String, ListBuffer[Int]]().asScala )
  //val modelIndex = Array( new ConcurrentHashMap[Int, TokenNGrams]().asScala, new ConcurrentHashMap[Int, TokenNGrams]().asScala)
  val criminalTokens = new ConcurrentHashMap[String, Int].keySet().asScala
  //val ccer = Config.ccer

  private def getAssociatedBlocks(idx: Int, dId: Int, textModelTokens: List[String], maxBlockSize: Array[Float]) = {
    val l, l0, r = List.newBuilder[(String, ListBuffer[Int])]
    val (mi, ii) = ConcurrentBlockingService.getIndexesForRetrieve(dId)
    for (t <- textModelTokens ; if !criminalTokens.contains(t)) {
      val block = ii(t)
      if (block.size == 0) l0.addOne((t, block))
      else (if (block.size+1 < maxBlockSize(dId)) l else r).addOne((t,block))
    }
    for ((t,b) <- r.result()) {
      criminalTokens.add(t)
      remove(t)
    }
    (l.result(), l0.result())
  }

  private def getIndexesForRetrieve(dId: Int) = { (dId, ccer) match {
      case (_, false) => (modelIndex(0), invertedIndex(0))
      case (0, true) => (modelIndex(1), invertedIndex(1))
      case (1, true) => (modelIndex(0), invertedIndex(0))
    }
  }

  private def getIndexesForUpdate(dId: Int) = { (dId, ccer) match {
      case (_, false) => (modelIndex(0), invertedIndex(0))
      case (1, true) => (modelIndex(1), invertedIndex(1))
      case (0, true) => (modelIndex(0), invertedIndex(0))
    }
  }

  def remove(tok: String) = {
    invertedIndex(0).remove(tok)
    invertedIndex(1).remove(tok)
  }
}
