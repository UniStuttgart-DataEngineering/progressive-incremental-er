package com.parER.core.prioritizing

import com.parER.core.blocking.EntityTokenBlockerRefiner
import com.parER.datastructure.BaseComparison
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class BlockSizePrioritizer(size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f) {

  val tokenBlocker = new EntityTokenBlockerRefiner(size1, size2, ro, ff)

  tokenBlocker.setModelStoring(false)

  val LBI = List ( new mutable.HashMap[String, ListBuffer[(Int, TokenNGrams)]], new mutable.HashMap[String, ListBuffer[(Int, TokenNGrams)]])
  val LCI = List ( new mutable.HashMap[String, Int], new mutable.HashMap[String, Int])
  val CS = mutable.SortedSet[String]()(Ordering.by[String, (Int, Int)](x => (LCI(0)(x) + LCI(1)(x), x.hashCode) ))
  //val LCS = List ( mutable.SortedSet()(Ordering.by[String, Int](LCI(0)(_))), mutable.SortedSet()(Ordering.by[String, Int](LCI(1)(_))))

  def countComparisons() = tokenBlocker.countComparisons()

  private def update(idx: Int, textModel: TokenNGrams) = {
    val dId = textModel.getDatasetId
    val (_, _, blocks) = tokenBlocker.progressiveProcess(idx, textModel)
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
        //assert(CS.contains(tok) == false)
        CI(tok) += (set.size-1)
        PI(tok).addOne((idx,textModel))
      } else {
        CI.addOne(tok, set.size)
        PI.addOne(tok, ListBuffer((idx, textModel)))
      }
      val oCI = LCI((dId+1)%2)
      if (!oCI.contains(tok)) {
        oCI.addOne(tok, 0)
      }
      if (CI(tok) >= 1 && oCI(tok) >= 1) {
        CS.addOne(tok)
      }
    }
  }

  def incrementalProcess(items: Seq[(Int, TokenNGrams)]) = {
    // Initialize data structures
    for ((idx, textModel) <- items) {
      update(idx, textModel)
    }
  }

  def getMinimum() = {

    if (!CS.isEmpty) {
      val bmin = CS.head
      val flag = CS.remove(bmin)
      assert(CS.contains(bmin) == false)
      val left = LBI(0)(bmin).result()   // unexecuted profiles of first dataset in bmin
      val right = LBI(1)(bmin).result()  // unexecuted profiles of second dataset in bmin

      val blockForLeft = tokenBlocker.getBlock(0, bmin)
      val blockForRight = tokenBlocker.getBlock(1, bmin)

      //TODO check if left is included in blockForRight
      //TODO check if right is included in blockForLeft



      LBI(0).remove(bmin)
      LBI(1).remove(bmin)
      LCI(0)(bmin) = 0
      LCI(1)(bmin) = 0

      (bmin, (left, blockForLeft), (right, blockForRight))

    } else {
      (null, (List(), List()), (List(),List()))
    }
  }

  def progressiveProcess(idx: Int, textModel: TokenNGrams) = {

    val dId = textModel.getDatasetId

    // update block collection
    val (_, _, blocks) = tokenBlocker.progressiveProcess(idx, textModel)


    val BI = LBI(dId)
    val CI = LCI(dId)
    //val CS = LCS(dId)

    // TODO not sure if efficient
    for (b <- blocks) {
      if (BI.contains(b._1)) {
        CS -= b._1
        CI(b._1) += b._2.size
        BI(b._1).addOne((idx,textModel))
        CS += b._1
      } else {
        CI.addOne(b._1, b._2.size)
        BI.addOne(b._1, ListBuffer((idx, textModel)))
        CS += b._1
      }
    }

    if (!CS.isEmpty) {
      val bmin = CS.head
      val profiles = BI(bmin) // TODO should check the minimum in LBI
      CS -= bmin
      BI -= bmin
      CI -= bmin

      // TODO check correctness?
      for (p <- profiles)
        assert(p._2.getDatasetId == dId)

      (dId, profiles.toList, (bmin, tokenBlocker.getBlock(idx, dId, bmin)))
    } else {
      (dId, List(), (null, List()))
    }
  }

  def isEmpty() = {
    CS.isEmpty
  }

  def getMinBlock() = {
    val bmin0 = if (CS.isEmpty) Int.MaxValue else LCI(0)(CS.head)
    val bmin1 = if (CS.isEmpty) Int.MaxValue else LCI(1)(CS.head)
    if (bmin0 < bmin1)
      getMinimumBlock(0)
    else
      getMinimumBlock(1)
  }

  def getMinimumBlock(dId: Int) = {

    val BI = LBI(dId)
    val CI = LCI(dId)
    //val CS = LCS(dId)

    if (!CS.isEmpty) {
      val bmin = CS.head
      val profiles = BI(bmin) // TODO should check the minimum in LBI
      CS -= bmin
      BI -= bmin
      CI -= bmin

      // TODO check correctness?
      for (p <- profiles)
        assert(p._2.getDatasetId == dId)

      (dId, profiles.toList, (bmin, tokenBlocker.getBlock(0, dId, bmin)))
    } else {
      (dId, List(), (null, List()))
    }
  }

}
