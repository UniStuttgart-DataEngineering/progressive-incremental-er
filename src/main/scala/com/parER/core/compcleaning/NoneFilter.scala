package com.parER.core.compcleaning

import com.elaunira.sbf.ScalableBloomFilter
import com.parER.datastructure.LightWeightComparison
import org.scify.jedai.textmodels.TokenNGrams

class NoneFilter() extends CompFiltering {

  println("NoneFilter")

  override def execute(comparisons: List[LightWeightComparison]) = {
    comparisons
  }

  override def execute(id: Int, model: TokenNGrams, ids: List[Int]): (Int, TokenNGrams, List[Int]) = (id, model, ids)

}
