package com.parER.core.compcleaning

import com.elaunira.sbf.ScalableBloomFilter
import com.parER.datastructure.BaseComparison
import org.scify.jedai.textmodels.TokenNGrams

class ScalableBFCompCleaner(val fpRate: Float) extends ComparisonCleaning {

  println("ScalableBFCompCleaner")
  var sbf = new ScalableBloomFilter[(Int,Int)](1000000, fpRate)

  protected def isRedundant(cmp: BaseComparison) : Boolean = {
    val p = (cmp.e1, cmp.e2)
    val res = sbf.contains(p)
    if (!res)
      sbf.add(p)
    res
  }

  override def execute(comparisons: List[BaseComparison]) = {
    for (cmp <- comparisons if !isRedundant(cmp)) yield cmp
  }

  override def execute(id: Int, model: TokenNGrams, ids: List[Int]): (Int, TokenNGrams, List[Int]) = (id, model, ids)

}
