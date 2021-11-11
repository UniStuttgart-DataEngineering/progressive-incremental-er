package com.parER.core.matching

import java.util
import java.util.{HashSet, Set}

import com.parER.core.blocking.{EntityTokenBlockerRefiner, TokenBlocker, TokenBlockerRefiner}
import com.parER.datastructure.BaseComparison

import scala.language.postfixOps
import scala.jdk.CollectionConverters._

object Matching {

  val JaccardSimilarity = (c: BaseComparison) => {
    if (c.e1Model == null || c.e2Model == null)
      throw new RuntimeException(s"Boom error in <${c.e1}, ${c.e1Model}, ${c.e2}, ${c.e2Model}")
    //val w = c.e1Model.getSimilarity(c.e2Model);
    val commonKeys: util.Set[String] = new util.HashSet[String](c.e1Model.getItemsFrequency.keySet)
    commonKeys.retainAll(c.e2Model.getItemsFrequency.keySet)
    val numerator: Float = commonKeys.size
    val denominator: Float = c.e1Model.getItemsFrequency.size + c.e2Model.getItemsFrequency.size - numerator
    c.sim = numerator / denominator
    c
  }

  val EditDistanceSimilarity = (c: BaseComparison) =>  {
    if (c.e1Model == null || c.e2Model == null)
      throw new RuntimeException(s"Boom error in <${c.e1}, ${c.e2}")
    val str1 = c.e1Model.getItemsFrequency.keySet().asScala
    val str2 = c.e2Model.getItemsFrequency.keySet().asScala
    val size = math.max(str1.size, str2.size).toFloat
    val ed = editDist(str1, str2).toFloat / size
    c.sim = ed
    c
  }

  def getMatcher(name: String) = name match {
    case "js" => {
      println("GETTING JACCARD SIMILARITY...")
      Matching.JaccardSimilarity
    }
    case "ed" => {
      println("GETTING EDIT DISTANCE SIMILARITY...")
      Matching.EditDistanceSimilarity
    }
  }

  // From : https://gist.github.com/tixxit/1246894/e79fa9fbeda695b9e8a6a5d858b61ec42c7a367d
  def editDist[A](a: Iterable[A], b: Iterable[A]) = {
    ((0 to b.size).toList /: a)((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => math.min(math.min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }) last
  }

}
