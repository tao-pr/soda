package de.tao.soda.etl

import com.typesafe.scalalogging.LazyLogging


trait Workflow[T1, T2] extends Serializable with LazyLogging {
  def run(input: T1): T2
  protected def prefixTree(level: Int) = if (level==0) "+--" else "|  "*level+"+--"
  def printTree(level: Int=0): String = s"${prefixTree(level)}${this.getClass.getSimpleName}"
}

trait IsoWorkflow[T] extends Workflow[T, T]

trait Generator[T] extends Workflow[Unit , T]
trait Multiplexer[T0, T1, T2] extends Workflow[T0, T1] {
  val self: Workflow[T0, T1]
  val plex: Workflow[T0, T2]

  override def run(input: T0): T1 = {
    val outSelf: T1 = self.run(input)
    val outPlex: T2 = plex.run(input)
    outSelf
  }

  override def printTree(level: Int): String = {
    val nlevel = level+1
    super.printTree(level) + "\n" +
      s"${prefixTree(nlevel)}[self]" + "\n" +
      self.printTree(nlevel+1) + "\n" +
      s"${prefixTree(nlevel)}[plex]" + "\n" +
      plex.printTree(nlevel+1)
  }
}

/**
 * Workflow that returns exactly what it intakes
 */
class IdentityWorkflow[T] extends IsoWorkflow[T] {
  override def run(input: T): T = input
}


