package com.parER.akka.streams.utils

import java.util.Comparator

import com.parER.datastructure.LightWeightComparison

object EntityComparator extends Comparator[LightWeightComparison] {
  override def compare(o1: LightWeightComparison, o2: LightWeightComparison): Int = {
          -java.lang.Float.compare(o1.sim, o2.sim)
  }
}