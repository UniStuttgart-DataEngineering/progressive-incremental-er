package com.parER.akka.streams.messages

import com.parER.datastructure.{BaseComparison, Comparison, LightWeightComparison}
import org.scify.jedai.prioritization.IPrioritization
import org.scify.jedai.textmodels.TokenNGrams

abstract class Message

case class Update(id: Int, model: TokenNGrams) extends Message

case class UpdateSeq(updateSeq: Seq[Update]) extends Message

case class Comparisons(comparisons: List[BaseComparison]) extends Message

case class IncrComparisons(incrIdx: Int, comparisons: List[BaseComparison]) extends Message

case class MessagePrioritization(prioritization: IPrioritization)

case class MessageComparisons(comparisons: List[LightWeightComparison]) extends Message

case class UpdateSeqAndCompSeq(updateSeq: Seq[Update], comparisons: List[BaseComparison]) extends Message

case class UpdateSeqAndMessageCompSeq(updateSeq: Seq[Update], comparisons: List[LightWeightComparison]) extends Message

case class UpdateSeqAndComparisonsSeq(updateSeq: Seq[Update], comparisonsSeq: ComparisonsSeq) extends Message

case class UpdateSeqAndMessageComparisonsSeq(updateSeq: Seq[Update], comparisonsSeq: MessageComparisonsSeq) extends Message

case class MessageComparisonsSeq(comparisonSeq: Seq[MessageComparisons]) extends Message

case class ComparisonsSeq(comparisonSeq: Seq[Comparisons]) extends Message

case class ComparisonsRequest(size: Int) extends Message

case class BlockTuple(id: Int, model: TokenNGrams, blocks: List[List[Int]]) extends Message

case class BlockTupleSeq(blockTupleSeq: Seq[BlockTuple]) extends Message

case class AggregatorMessage(id: String, e1: Int, e1Model: TokenNGrams, blocks: List[List[Int]])

case class Terminate() extends Message

case class NoBlockComparisons() extends Message