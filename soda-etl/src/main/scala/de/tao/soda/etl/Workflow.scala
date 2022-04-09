package de.tao.soda.etl

trait Workflow[T1, T2] extends Serializable {
  def run(input: T1, dry: Boolean=false): T2
}

trait IsoWorkflow[T] extends Workflow[T, T]

private case object Nothing

trait Generator[T] extends Workflow[Nothing, T]
trait Terminator[T] extends Workflow[T, Nothing]
trait Multiplexer[T0, T1, T2] extends Workflow[T0, T1] {
  val self: Workflow[T0, T1]
  val plex: Workflow[T0, T2]

  override def run(input: T0, dry: Boolean): T1 = {
    val outSelf: T1 = self.run(input, dry)
    val outPlex: T2 = plex.run(input, dry)
    outSelf
  }
}

object Workflow {
  // TODO:

  def fromFile(file: String): Workflow[_, _] = ???
}