package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages._
import com.parER.core.blocking.StoreModel
import com.parER.datastructure.{BaseComparison, LightWeightComparison}

class StoreSeqModelStageWithIncrCount(val size1: Int = 16, val size2: Int = 16) extends GraphStage[FlowShape[Message, IncrComparisons]] {
  val in = Inlet[Message]("StoreSeqModelStage.in")
  val out = Outlet[IncrComparisons]("StoreSeqModelStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val storeModel = new StoreModel(size1, size2)
      var delayComparisons = List[BaseComparison]()
      var delayMessageComparisons = List[LightWeightComparison]()
      var count = 0
      var incrCount = 0

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val items   = grab(in)

          items match {
            case UpdateSeq(updates) => {
              incrCount += 1
              updates.map( u => storeModel.solveUpdate(u.id, u.model) )
              if (!hasBeenPulled(in)) {
                pull(in)
              }
            }

            case MessageComparisons(mlc) =>
              try {
                val llc = if (delayMessageComparisons.isEmpty) storeModel.betterSolveMessageComparisons(mlc)
                else storeModel.betterSolveMessageComparisons(delayMessageComparisons++mlc)
                if (llc.size > 0) {
                  count += llc.size
                  //                  if (count % 1000 == 0)
                  //                    println("StoreSeqModelStage: " + count)
                  push(out, IncrComparisons(incrCount, llc))
                } else {
                  if (!hasBeenPulled(in)) {
                    pull(in)
                  }
                }
              } catch {
                case e => {
                  println("Key not found exception...");
                  delayMessageComparisons ++= mlc
                  if (!hasBeenPulled(in)) {
                    pull(in)
                  }
                }
              }

            case Comparisons(lc) =>
              try {
                val llc = if (delayComparisons.isEmpty) storeModel.betterSolveComparisons(lc)
                else storeModel.betterSolveComparisons(delayComparisons++lc)
                if (llc.size > 0) {
                  count += llc.size
//                  if (count % 1000 == 0)
//                    println("StoreSeqModelStage: " + count)
                  push(out, IncrComparisons(incrCount, llc))
                } else {
                  if (!hasBeenPulled(in)) {
                    pull(in)
                  }
                }
              } catch {
                case e => {
                  println("Key not found exception...");
                  delayComparisons ++= lc
                  if (!hasBeenPulled(in)) {
                    pull(in)
                  }
                }
              }

            case ComparisonsSeq(s) => {
              val llc = List.newBuilder[BaseComparison]
              for (lc <- s) {
                val comps = storeModel.betterSolveComparisons(lc.comparisons)
                llc ++= comps
              }
              if (llc.knownSize > 0) {
                push(out, IncrComparisons(incrCount, llc.result()))
              } else {
                if (!hasBeenPulled(in)) {
                  pull(in)
                }
              }
            }

            case UpdateSeqAndMessageCompSeq(updates, mlc) => {
              incrCount += 1
              updates.map( u => storeModel.solveUpdate(u.id, u.model) )
              try {
                val llc = if (delayComparisons.isEmpty) storeModel.betterSolveMessageComparisons(mlc)
                else storeModel.betterSolveMessageComparisons(delayMessageComparisons++mlc)
                if (llc.size > 0) {
                  //                  count += llc.size
                  //                  if (count % 1000 == 0)
                  //                    println("StoreSeqModelStage[U]: " + count)
                  push(out, IncrComparisons(incrCount, llc))
                } else {
                  if (!hasBeenPulled(in)) {
                    pull(in)
                  }
                }
              } catch {
                case e => {
                  println("Key not found exception...");
                  delayMessageComparisons ++= mlc
                  if (!hasBeenPulled(in)) {
                    pull(in)
                  }
                }
              }
            }

            case UpdateSeqAndCompSeq(updates, lc) => {
              incrCount += 1
              updates.map( u => storeModel.solveUpdate(u.id, u.model) )
              try {
                val llc = if (delayComparisons.isEmpty) storeModel.betterSolveComparisons(lc)
                else storeModel.betterSolveComparisons(delayComparisons++lc)
                if (llc.size > 0) {
//                  count += llc.size
//                  if (count % 1000 == 0)
//                    println("StoreSeqModelStage[U]: " + count)
                  push(out, IncrComparisons(incrCount, llc))
                } else {
                  if (!hasBeenPulled(in)) {
                    pull(in)
                  }
                }
              } catch {
                case e => {
                  println("Key not found exception...");
                  delayComparisons ++= lc
                  if (!hasBeenPulled(in)) {
                    pull(in)
                  }
                }
              }
            }

            case _ => {
              println("SOME MESSAGE UNEXPECTED WTF")
            }
          }
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (!hasBeenPulled(in)) {
            pull(in)
          }
        }
      })
    }

}