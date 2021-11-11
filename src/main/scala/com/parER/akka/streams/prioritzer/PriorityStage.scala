package com.parER.akka.streams.prioritzer

import akka.stream.FlowShape
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Unzip}
import com.parER.core.Config
import com.parER.core.matching.SchemeJSMatcher
import com.parER.datastructure.{BaseComparison, Comparison}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class PriorityStage {

}

object PriorityStage {
  def apply(name: String, opt: Long = 0, opt2: Long = 1) = name match {
    case "no" => new NoStage(name, opt)
    case "pps" => new PPSStage(name, opt.toInt, opt2.toInt)
    case "dpps" =>  (GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

        // prepare graph elements
      val partitioner = b.add(Flow[List[BaseComparison]].map(ppsPartitioner))
      val ppsStage = new DecoupledPPSStage("pps", opt.toInt, opt2.toInt)
      val unzip = b.add(Unzip[List[BaseComparison], List[BaseComparison]])
      val merge = b.add(Merge[List[BaseComparison]](2, false))
      partitioner ~> unzip.in
      unzip.out0 ~> merge
      unzip.out1 ~> ppsStage.async ~> merge
      FlowShape(partitioner.in, merge.out)
    })
  }

  def ppsPartitioner(comparisons: List[BaseComparison]) = {
    val kMax = Config.pOption
    val pq = mutable.PriorityQueue()(Ordering.by[BaseComparison, Float](_.sim).reverse)
    val matcher = new SchemeJSMatcher
    matcher.execute(comparisons)
    val nComparisons = new ListBuffer[BaseComparison]()
    for (cmp <- comparisons) {
      pq.enqueue(cmp)
      if (kMax < pq.size) {
        nComparisons += pq.dequeue()
      }
    }
    (pq.toList, nComparisons.result())
  }
}

