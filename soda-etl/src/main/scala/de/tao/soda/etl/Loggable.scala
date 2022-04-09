package de.tao.soda.etl

trait Loggable {
  lazy val logger = org.log4s.getLogger
}
