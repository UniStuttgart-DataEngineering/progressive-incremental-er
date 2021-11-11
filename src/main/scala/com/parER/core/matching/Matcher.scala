package com.parER.core.matching

import com.parER.datastructure.{BaseComparison, Comparison}

trait Matcher {
  def execute(comparisons: List[BaseComparison]): List[BaseComparison]
  def execute(BaseComparison: BaseComparison) : BaseComparison
  def getName(): String
}

object Matcher {
  def apply(name: String) = name match {
    case "no" => null
    case "js" => new JSMatcher
  }
}
