package com.parER.akka.streams.utils

class RatingControl(size: Int) {

  val DMeasures = Array.fill[Long](size)(0)
  val UMeasures = Array.fill[Long](size)(0)
  val SMeasures = Array.fill[Long](size)(0)
  var diTot, uiTot, siTot = 0L
  var di, ui, si = 0

  def updateDSTime(lastTime: Long) = {
    val last = DMeasures(di)
    DMeasures(di) = lastTime
    diTot = diTot - last + lastTime
    di = (di+1) % size
  }

  def updateUSTime(lastTime: Long) = {
    val last = UMeasures(ui)
    UMeasures(ui) = lastTime
    uiTot = uiTot - last + lastTime
    ui = (ui+1) % size
  }

  def updateTSTime(lastTime: Long) = {
    val last = SMeasures(si)
    SMeasures(si) = lastTime
    siTot = siTot - last + lastTime
    si = (si+1) % size
  }

  def getDSTime = diTot.toFloat/size
  def getUSTime = uiTot.toFloat/size
  def getTSTime = siTot.toFloat/size

}
