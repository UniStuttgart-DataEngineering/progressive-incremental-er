package com.parER.utils

import java.util
import java.util.List

import com.parER.core.Config
import com.parER.core.matching.Matching
import com.parER.progressive.main.JedaiProgressiveCCMain.{profiles1, profiles2}
import org.scify.jedai.datamodel.{Comparison, EntityProfile}
import org.scify.jedai.entitymatching.ProfileMatcher
import org.scify.jedai.textmodels.TokenNGrams
import org.scify.jedai.utilities.enumerations.{RepresentationModel, SimilarityMetric}

class ExtendedProfileMatcher(profilesD1: util.List[EntityProfile], profilesD2: util.List[EntityProfile], model: RepresentationModel, simMetric: SimilarityMetric) extends ProfileMatcher(profilesD1, profilesD2, model, simMetric){

  val matcherFun = Matching.getMatcher(Config.matcher)

  override def executeComparison(comparison: Comparison): Float = {
    if (isCleanCleanER) {
      val bc1 = com.parER.datastructure.Comparison(comparison.getEntityId1, entityModelsD1(comparison.getEntityId1).asInstanceOf[TokenNGrams], comparison.getEntityId2, entityModelsD2(comparison.getEntityId2).asInstanceOf[TokenNGrams])
      matcherFun(bc1).sim
    } else {
      val bc1 = com.parER.datastructure.Comparison(comparison.getEntityId1, entityModelsD1(comparison.getEntityId1).asInstanceOf[TokenNGrams], comparison.getEntityId2, entityModelsD1(comparison.getEntityId2).asInstanceOf[TokenNGrams])
      matcherFun(bc1).sim
    }
  }

}
