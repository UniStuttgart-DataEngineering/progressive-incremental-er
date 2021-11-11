package com.parER.core.compcleaning

import bloomfilter.mutable.BloomFilter
import com.elaunira.sbf.SlicedBloomFilter
import com.parER.datastructure.LightWeightComparison
import org.scify.jedai.textmodels.TokenNGrams

class BFFilter(val capacity: Int, val fpRate: Float) extends CompFiltering {

  println("BFFilter")
  var sbf = new BloomFilter[Long](capacity, 2)

  def mergePair(e1: Int, e2: Int) : Long = {
    val l = (e1.toLong << 32) | (e2 & 0xffffffffL)
    return l
  }

  protected def isRedundant(cmp: LightWeightComparison) : Boolean = {
    val p = mergePair(cmp.e1, cmp.e2)
    //val res = sbf.contains(p)
    val res = sbf.mightContain(p)
    if (!res)
      sbf.add(p)
    res
  }

  override def execute(comparisons: List[LightWeightComparison]) = {
    for (cmp <- comparisons if !isRedundant(cmp)) yield cmp
  }

  override def execute(id: Int, model: TokenNGrams, ids: List[Int]): (Int, TokenNGrams, List[Int]) = (id, model, ids)

}
