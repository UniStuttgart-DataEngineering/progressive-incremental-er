package com.parER.akka.streams

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.parER.core.Config
import com.parER.core.collecting.{ProgressiveCollector, ProgressiveCollectorWithStatistics}
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation

import scala.concurrent.{Future, Promise}

class ProgressiveCollectorSink(t0: Long, t1: Long, dp: AbstractDuplicatePropagation, print: Boolean = true, incremental: Boolean = false, subParts: Int = 1) extends GraphStageWithMaterializedValue[SinkShape[List[BaseComparison]], Future[Long]] {
  val in: Inlet[List[BaseComparison]] = Inlet("CollectorSink")
  override val shape: SinkShape[List[BaseComparison]] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val p: Promise[Long] = Promise()
    val logic = new GraphStageLogic(shape) {

      //val proColl = new ProgressiveCollector(t0, t1, dp, print)
      val proColl = new ProgressiveCollectorWithStatistics(t0, t1, dp, print, incremental, parts = subParts)

      var counter = 0L;

      // This requests one element at the Sink startup.
      override def preStart(): Unit = pull(in)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val comparisons = grab(in)
          counter += comparisons.size
          //if (counter % 100 == 0)
          //  println(counter)
          proColl.execute(comparisons)
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          val result = counter
          System.out.println("The very last...")
          proColl.printLast()
          proColl.writeFile(s"test-outputs/${Config.name}-${Config.groundtruth}-${Config.matcher}.txt")
          proColl.printEC()
          p.trySuccess(result)
          completeStage()
        }
      })
    }
    (logic, p.future)
  }
}
