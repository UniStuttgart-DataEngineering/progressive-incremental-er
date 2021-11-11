package com.parER.utils

import com.parER.core.compcleaning.NoJedaiComparisonCleaning
import org.scify.jedai.blockprocessing.comparisoncleaning._
import org.scify.jedai.utilities.enumerations.WeightingScheme

object JedaiComparisonCleaning {
  def apply(name: String, wScheme: WeightingScheme) = name match {
    // TODO do that for all CC methods in jedai
    case "no" => new NoJedaiComparisonCleaning
    case "hs" => new ComparisonPropagation
    case "wnp" => new WeightedNodePruning(wScheme)
    case "cnp" => new CardinalityNodePruning(wScheme)
    case "wep" => new WeightedEdgePruning(wScheme)
    case "cep" => new CardinalityEdgePruning(wScheme)
    case "rcnp" => new ReciprocalCardinalityNodePruning(wScheme)
    case "rwnp" => new ReciprocalWeightedNodePruning(wScheme)
    case "rcnpjs" => new ReciprocalWeightedNodePruning(WeightingScheme.JS)
    case "rcnparcs" => new ReciprocalWeightedNodePruning(WeightingScheme.ARCS)
  }
}
