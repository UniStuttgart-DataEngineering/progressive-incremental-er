package com.parER.core.compcleaning

import java.util

import org.apache.jena.atlas.json.JsonArray
import org.scify.jedai.blockprocessing.comparisoncleaning.AbstractComparisonCleaning
import org.scify.jedai.datamodel.AbstractBlock

class NoJedaiComparisonCleaning extends AbstractComparisonCleaning {
  override def applyMainProcessing(): util.List[AbstractBlock] = ???

  override def getNumberOfGridConfigurations: Int = 1

  override def setNextRandomConfiguration(): Unit = {}

  override def setNumberedGridConfiguration(iterationNumber: Int): Unit = 1

  override def setNumberedRandomConfiguration(iterationNumber: Int): Unit = 1

  override def getMethodConfiguration: String = ""

  override def getMethodInfo: String = ""

  override def getMethodName: String = ""

  override def getMethodParameters: String = ""

  override def getParameterConfiguration: JsonArray = new JsonArray

  override def getParameterDescription(parameterId: Int): String = ""

  override def getParameterName(parameterId: Int): String = ""

  override def refineBlocks(blocks: util.List[AbstractBlock]): util.List[AbstractBlock] = blocks
}
