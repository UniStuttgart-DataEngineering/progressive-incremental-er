package com.parER.utils

import com.parER.progressive.main.CepCnpEntities
import org.scify.jedai.prioritization.{ProgressiveBlockScheduling, ProgressiveEntityScheduling, ProgressiveGlobalTopComparisons, ProgressiveLocalTopComparisons}
import org.scify.jedai.utilities.enumerations.WeightingScheme

object JedaiPrioritization {
  def apply(name: String, profiles: Int, totalComparisons: Int, wScheme: WeightingScheme) = name match {
    // TODO do that for all CC methods in jedai
    case "pps" => { new ProgressiveEntityScheduling(totalComparisons, wScheme) }
    case "pps-local" => { new ProgressiveEntityScheduling(totalComparisons, wScheme) }
    case "pes" => new ProgressiveEntityScheduling(totalComparisons, wScheme)
    case "pbs" => new ProgressiveBlockScheduling(totalComparisons, wScheme)
    case "pgtc" => new ProgressiveGlobalTopComparisons(totalComparisons, wScheme)
    case "pltc" => new ProgressiveLocalTopComparisons(totalComparisons, wScheme)
    //case "gspsn" => new TODO
    //case "lspsn" => new TODO
    case "cepcnpent" => new CepCnpEntities(wScheme, profiles)
  }
}
