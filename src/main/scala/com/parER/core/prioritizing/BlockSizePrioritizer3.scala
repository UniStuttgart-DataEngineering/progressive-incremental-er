package com.parER.core.prioritizing

import com.parER.core.Config
import com.parER.core.blocking.EntityTokenBlockerRefiner
import com.parER.datastructure.LightWeightComparison
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class BlockSizePrioritizer3(size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f) {

  val ccer = Config.ccer

  println("BlockSizePrioritizer3: without TokenNGrams + sorting only when asked!")
  val tokenBlocker = new EntityTokenBlockerRefiner(size1, size2, ro, ff)

  tokenBlocker.setModelStoring(false)

  // Unexecuted entities per block
  val LBI = List ( new mutable.HashMap[String, ListBuffer[Int]], new mutable.HashMap[String, ListBuffer[Int]])
  // Comparison Index
  val LCI = List ( new mutable.HashMap[String, Int], new mutable.HashMap[String, Int])
  val notUpdated = mutable.HashSet[(String, Int)]()
  val CS = mutable.SortedSet[String]()(Ordering.by[String, (Int, Int)](x => (LCI(0)(x) + LCI(1)(x), x.hashCode) ))

  //val LCS = List ( mutable.SortedSet()(Ordering.by[String, Int](LCI(0)(_))), mutable.SortedSet()(Ordering.by[String, Int](LCI(1)(_))))
  val BlockIndex = new mutable.HashMap[(Int,Int), ListBuffer[Int]]

  var counter = 0

  def countComparisons() = tokenBlocker.countComparisons()

  def countBlockComparisons(key: String) = {
    val s1 = tokenBlocker.getBlock(0, key).size
    if (ccer)
      s1 * tokenBlocker.getBlock(1, key).size
    else
      s1 * (s1-1) / 2
  }

  def approxCBS(c:LightWeightComparison, bmin: String) = {
    val t1 = (c.e1, 0)
    val t2 = if (ccer) (c.e2, 1) else (c.e2, 0)
    val key = bmin.hashCode
    if (!BlockIndex.contains(t1))
      BlockIndex.addOne{(t1, ListBuffer(key))}
    if (!BlockIndex.contains(t2))
      BlockIndex.addOne{(t2, ListBuffer(key))}
    if (!BlockIndex(t1).contains(key))
      BlockIndex(t1) += key
    if (!BlockIndex(t2).contains(key))
      BlockIndex(t2) += key
    BlockIndex(t1).intersect(BlockIndex(t2)).size.toFloat
  }

  def updateBlockIndex(tuple: (Int, Int), blocks: List[String]) = {
      if (BlockIndex.contains(tuple)) {
        val intBlocks = blocks.map(_.hashCode)
        BlockIndex(tuple) --= intBlocks
        BlockIndex(tuple) ++= intBlocks
      } else {
        val intBlocks = blocks.map(_.hashCode)
        BlockIndex.addOne{(tuple, ListBuffer()++intBlocks)}
      }
  }

  private def updateCS() = {
    if (notUpdated.size > 0) {
      for (tok_dId <- notUpdated) {
        val oCI = LCI((tok_dId._2 + 1) % 2)
        val CI = LCI(tok_dId._2)
        if (CI(tok_dId._1) >= 1 && (!ccer || oCI(tok_dId._1) >= 1)) {
          CS.addOne(tok_dId._1) // SORTING
        }
      }
      notUpdated.clear()
      assert(notUpdated.size == 0)
    }
  }

  private def update(idx: Int, textModel: TokenNGrams) = {
    val dId = textModel.getDatasetId
    val (_, _, blocks) = tokenBlocker.progressiveProcess(idx, textModel)
    if (blocks.size > 0) {
      updateBlockIndex((idx, textModel.getDatasetId), blocks.map(_._1))
    }
    val PI = LBI(dId)
    val CI = LCI(dId)
    //val CS = LCS(dId)

    // TODO not sure if efficient
    // blocks refer to the blocks associated to (idx, textModel)
    for (block <- blocks) {
      val tok = block._1
      val set = block._2
      if (PI.contains(tok)) {
        CS.remove(tok)
//        if (!( flag || notUpdated.contains((tok, 0)) || notUpdated.contains((tok,1)) )) {
//          val toset = CS.toList
//          println(PI(tok))
//          println(CI(tok))
//          println( toset.contains(tok) )
//          assert( flag || notUpdated.contains((tok, 0)) || notUpdated.contains((tok,1)) )                                       // SORTING
//        }
        notUpdated.add((tok, dId))
        CI(tok) += (if (ccer) set.size else set.size-1)
        PI(tok).addOne(idx)
      } else {
        notUpdated.add((tok, dId))
        CI.addOne(tok, (if (ccer) set.size else set.size-1))
        PI.addOne(tok, ListBuffer(idx))
      }
      val oCI = LCI((dId + 1) % 2)
      if (!oCI.contains(tok)) {
        oCI.addOne(tok, 0)
      }
//      if (CI(tok) >= 1 && (!ccer || oCI(tok) >= 1)) {
//        CS.addOne(tok)                                      // SORTING
//      }
    }
  }

  def incrementalProcess(items: Seq[(Int, TokenNGrams)]) = {
    // Initialize data structures
    for ((idx, textModel) <- items) {
      update(idx, textModel)
    }
  }

  def getMinimumComparisons() = {
    updateCS()
    val bmin = CS.head
    LCI(0)(bmin) + LCI(1)(bmin)
  }

  def getMinimumBlockSize() = {
    updateCS()
    val bmin = if (!CS.isEmpty) CS.head else null
    if (bmin != null)
      countBlockComparisons(bmin)
    else
      1
  }

  def getMinimum() = {
    updateCS()
    if (!CS.isEmpty) {
      val bmin = CS.head
      val flag = CS.remove(bmin)                          // SORTING
      assert(CS.contains(bmin) == false)
      val left = LBI(0)(bmin).result()   // unexecuted profiles of first dataset in bmin
      val right = if (ccer) LBI(1)(bmin).result() else null // unexecuted profiles of second dataset in bmin

      val blockForLeft = tokenBlocker.getBlock(0, bmin)
      val blockForRight = if (ccer) tokenBlocker.getBlock(1, bmin) else null

      //TODO check if left is included in blockForRight
      //TODO check if right is included in blockForLeft

      if (ccer) {
        LBI(0).remove(bmin)
        LBI(1).remove(bmin)
        LCI(0)(bmin) = 0
        LCI(1)(bmin) = 0
      } else {
        LBI(0).remove(bmin)
        LCI(0)(bmin) = 0
      }

      (bmin, (left, blockForLeft), (right, blockForRight))

    } else {
      (null, (List(), List()), (List(),List()))
    }
  }

//  def progressiveProcess(idx: Int, textModel: TokenNGrams) = {
//
//    val dId = textModel.getDatasetId
//
//    // update block collection
//    val (_, _, blocks) = tokenBlocker.progressiveProcess(idx, textModel)
//
//
//    val BI = LBI(dId)
//    val CI = LCI(dId)
//    //val CS = LCS(dId)
//
//    // TODO not sure if efficient
//    for (b <- blocks) {
//      if (BI.contains(b._1)) {
//        CS -= b._1
//        CI(b._1) += b._2.size
//        BI(b._1).addOne((idx,textModel))
//        CS += b._1
//      } else {
//        CI.addOne(b._1, b._2.size)
//        BI.addOne(b._1, ListBuffer((idx, textModel)))
//        CS += b._1
//      }
//    }
//
//    if (!CS.isEmpty) {
//      val bmin = CS.head
//      val profiles = BI(bmin) // TODO should check the minimum in LBI
//      CS -= bmin
//      BI -= bmin
//      CI -= bmin
//
//      // TODO check correctness?
//      for (p <- profiles)
//        assert(p._2.getDatasetId == dId)
//
//      (dId, profiles.toList, (bmin, tokenBlocker.getBlock(idx, dId, bmin)))
//    } else {
//      (dId, List(), (null, List()))
//    }
//  }

  def isEmpty() = {
    updateCS()
    CS.isEmpty
  }

//  def getMinBlock() = {
//    val bmin0 = if (CS.isEmpty) Int.MaxValue else LCI(0)(CS.head)
//    val bmin1 = if (CS.isEmpty) Int.MaxValue else LCI(1)(CS.head)
//    if (bmin0 < bmin1)
//      getMinimumBlock(0)
//    else
//      getMinimumBlock(1)
//  }
//
//  def getMinimumBlock(dId: Int) = {
//
//    val BI = LBI(dId)
//    val CI = LCI(dId)
//    //val CS = LCS(dId)
//
//    if (!CS.isEmpty) {
//      val bmin = CS.head
//      val profiles = BI(bmin) // TODO should check the minimum in LBI
//      CS -= bmin
//      BI -= bmin
//      CI -= bmin
//
//      // TODO check correctness?
//      for (p <- profiles)
//        assert(p._2.getDatasetId == dId)
//
//      (dId, profiles.toList, (bmin, tokenBlocker.getBlock(0, dId, bmin)))
//    } else {
//      (dId, List(), (null, List()))
//    }
//  }

}
