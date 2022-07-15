package de.tao.soda

object Domain {

  case class SimpleRow(a: Int, s: String)

  case class PolishedRow(a: Int, s: String, value: Double)

  case class SubBracket(a: Int, uid: String, vec: Array[Double])

  case class Event(uid: String, dt: String, v: Double)

  case class Bracket(uid: String, sub: Array[SubBracket], events: Array[Event])
}
