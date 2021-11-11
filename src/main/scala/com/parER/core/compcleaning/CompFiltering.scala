package com.parER.core.compcleaning

import com.parER.datastructure.{BaseComparison, LightWeightComparison}
import org.scify.jedai.textmodels.TokenNGrams

trait CompFiltering {
  def execute(comparisons: List[LightWeightComparison]) : List[LightWeightComparison]
  def execute(id: Int, model: TokenNGrams, ids: List[Int]) : (Int, TokenNGrams, List[Int])
}


