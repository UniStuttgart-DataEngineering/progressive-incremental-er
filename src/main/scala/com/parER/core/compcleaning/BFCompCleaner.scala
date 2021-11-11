package com.parER.core.compcleaning

import bloomfilter.mutable.BloomFilter
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.textmodels.TokenNGrams

class BFCompCleaner(val fpRate: Float) extends ComparisonCleaning {
  //TODO adapt for clean-clean ER
  var idx = -1
  var bf : BloomFilter[Long] = null

  def mergePair(e1: Int, e2: Int) : Long = {
    val l = (e1.toLong << 32) | (e2 & 0xffffffffL)
    return l
  }

  protected def isRedundant(cmp: BaseComparison) : Boolean = {
    // by construction e1 < e2
    val long = mergePair(cmp.e1, cmp.e2)
    val res = bf.mightContain(long)
    if (!res) bf.add(long)
    res
  }

  override def execute(comparisons: List[BaseComparison]) = {
    assert(comparisons.isEmpty || comparisons(0).e2 == comparisons.last.e2)
    if (!comparisons.isEmpty && idx != comparisons(0).e2) {
      if ( bf != null ) bf.dispose()
      bf = BloomFilter[Long](comparisons.size, fpRate)
      idx = comparisons(0).e2
    }
    for (cmp <- comparisons if !isRedundant(cmp)) yield cmp
  }

  override def execute(id: Int, model: TokenNGrams, ids: List[Int]): (Int, TokenNGrams, List[Int]) = (id, model, ids)

}
