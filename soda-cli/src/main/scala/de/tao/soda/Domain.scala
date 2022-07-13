package de.tao.soda

object Domain {

  case class SimpleRow(a: Int, s: String)

  case class PolishedRow(a: Int, s: String, value: Double)
}
