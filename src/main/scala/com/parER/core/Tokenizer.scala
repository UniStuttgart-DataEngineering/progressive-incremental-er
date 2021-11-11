package com.parER.core

import org.scify.jedai.datamodel.EntityProfile
import org.scify.jedai.textmodels.TokenNGrams
import org.scify.jedai.utilities.enumerations.{RepresentationModel, SimilarityMetric}

/**
 *
 * This class builds a text model Token-1-grams for the blocker
 *
 */
class Tokenizer { // TODO move tokenizer as object? i.e. static method -> may be useful for akka
  def execute(index: Int, datasetIndex:Int, profile: EntityProfile) = {
    val textModel = new TokenNGrams(datasetIndex, 1, RepresentationModel.TOKEN_UNIGRAMS, SimilarityMetric.JACCARD_SIMILARITY, profile.getEntityUrl)
    import scala.jdk.CollectionConverters._
    val attributes = profile.getAttributes.asScala
    for (attr <- attributes) {
      textModel.updateModel(attr.getValue)
    }
    textModel.finalizeModel
    (index, textModel)
  }

  private def getBlockingKeys(attributeValue: String) = {
    List[String](getTokens(attributeValue).toSeq : _*)
  }

  private def getTokens(attributeValue: String) = {
    attributeValue.split("[\\W_]")
  }
}
