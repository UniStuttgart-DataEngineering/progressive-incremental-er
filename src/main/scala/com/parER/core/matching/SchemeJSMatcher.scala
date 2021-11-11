package com.parER.core.matching

import com.parER.datastructure.{BaseComparison, Comparison}

class SchemeJSMatcher extends Matcher {
  override def execute(comparisons: List[BaseComparison]) = {
    comparisons.map(execute(_))
  }

  override def execute(cmp: BaseComparison): BaseComparison = {
    //cmp.sim = ( cmp.counters(0) + cmp.counters(1) ) / ( cmp.e1Model.getItemsFrequency.size() + cmp.e2Model.getItemsFrequency.size() - cmp.counters(0) - cmp.counters(1) )
    //cmp.sim = cmp.counters(0) + cmp.counters(1)
    cmp
  }

  override def getName(): String = "SchemeJSMatcher"
}
