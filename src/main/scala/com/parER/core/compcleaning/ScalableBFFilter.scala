package com.parER.core.compcleaning

import com.elaunira.sbf.ScalableBloomFilter
import com.parER.datastructure.LightWeightComparison
import org.scify.jedai.textmodels.TokenNGrams

class ScalableBFFilter(val initialCapacity: Int, val fpRate: Float) extends CompFiltering {

  println("ScalableBFFilter")
  var sbf = new ScalableBloomFilter[(Int,Int)](initialCapacity, fpRate)

  def handleException() = {
    println("Reset scalable bloom filter...")
    sbf = new ScalableBloomFilter[(Int,Int)](initialCapacity, fpRate)
  }

  protected def isRedundant(cmp: LightWeightComparison) : Boolean = {
    val p = (cmp.e1, cmp.e2)
    val res = sbf.contains(p)
    try {
      if (!res)
        sbf.add(p)
    } catch {
      case nase: NegativeArraySizeException => {
        println("SBF: negative array size exception")
        handleException()
      }
      case th: Throwable => {
        println("Another throwable exception.")
        println(s"${th.getMessage}")
        th.printStackTrace()
        handleException()
      }
    }
    res
  }

  override def execute(comparisons: List[LightWeightComparison]) = {
    for (cmp <- comparisons if !isRedundant(cmp)) yield cmp
  }

  override def execute(id: Int, model: TokenNGrams, ids: List[Int]): (Int, TokenNGrams, List[Int]) = (id, model, ids)

}
