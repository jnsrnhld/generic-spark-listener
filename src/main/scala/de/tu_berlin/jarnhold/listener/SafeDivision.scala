package de.tu_berlin.jarnhold.listener

import scala.util.Try

object SafeDivision {

  def saveDivision(a: Int, b: Int): Double = {
    saveDivision(a.toDouble, b.toDouble)
  }

  def saveDivision(a: Long, b: Long): Double = {
    saveDivision(a.toDouble, b.toDouble)
  }

  def saveDivision(a: Double, b: Double): Double = {
    if (a == 0.0) {
      0.0
    }
    else if (b == 0.0) {
      1.0
    }
    else {
      Try(a / b).getOrElse(0.0)
    }
  }
}
