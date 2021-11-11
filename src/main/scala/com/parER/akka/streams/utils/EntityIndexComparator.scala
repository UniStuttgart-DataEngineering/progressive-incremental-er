package com.parER.akka.streams.utils

import java.util.Comparator

import com.parER.datastructure.LightWeightComparison

object EntityIndexComparator extends Comparator[(Int, Float)] {
  override def compare(o1: (Int, Float), o2: (Int, Float)): Int = {
    -java.lang.Float.compare(o1._2, o2._2)
  }
}