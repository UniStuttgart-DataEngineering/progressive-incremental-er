package com.parER.datastructure

import org.scify.jedai.textmodels.TokenNGrams

trait LightWeightComparison {
  def e1: Int
  def e2: Int
  var sim: Float
}

case class MessageComparison(e1: Int, e2: Int, var sim : Float = 0.0f) extends LightWeightComparison

case class KeyComparison(e1: Int, e2: Int, var sim: Float, val key: String) extends LightWeightComparison

trait BaseComparison extends LightWeightComparison {
  def e1: Int
  def e1Model: TokenNGrams
  def e2: Int
  def e2Model: TokenNGrams
  var sim: Float
}

case class Comparison(val e1: Int, val e1Model: TokenNGrams, val e2: Int, val e2Model: TokenNGrams, var sim : Float = 0.0f) extends BaseComparison {
  //val counters : Array[Int] = Array(0,0)
}

case class ProgressiveComparison(val e1: Int, val e1Model: TokenNGrams, val e2: Int, val e2Model: TokenNGrams, var sim : Float = 0.0f, val key : String, val dID : Int = 0) extends BaseComparison {}