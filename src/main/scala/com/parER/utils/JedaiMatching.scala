package com.parER.utils

object JedaiMatching {
  def apply(name: String) = name match {
    // TODO do that for all CC methods in jedai
    case "no" => null
    case _ => "boh"
  }
}
