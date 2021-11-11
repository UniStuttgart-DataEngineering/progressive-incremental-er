package com.parER.core.blocking

import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.textmodels.TokenNGrams

trait Blocking {
  def execute(idx: Int, textModel: TokenNGrams) : List[BaseComparison]
  def execute(idx: Int, textModel: TokenNGrams, keys: List[String]) : List[BaseComparison]
  def setModelStoring(value: Boolean) : Unit
  def process(idx: Int, textModel: TokenNGrams) : (Int, TokenNGrams, List[List[Int]])
  def process(idx: Int, textModel: TokenNGrams, keys: List[String]) : (Int, TokenNGrams, List[List[Int]])
  def progressiveProcess(idx: Int, textModel: TokenNGrams) : (Int, TokenNGrams, List[(String,List[Int])])
  def progressiveProcess(idx: Int, textModel: TokenNGrams, keys: List[String]) : (Int, TokenNGrams, List[(String,List[Int])])
  def countComparisons() : Int
}

object Blocking {
  def apply(name: String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f) = name match {
    case "tb" => new TokenBlocker()
    case "tbr" => new TokenBlockerRefiner(size1, size2, ro, ff)
    case "etbr" => new EntityTokenBlockerRefiner(size1, size2, ro, ff)
  }
}