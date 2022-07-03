package de.tao.soda

import de.tao.soda.etl.DataLoader

import java.io.InvalidClassException

object Preset {
  val appHM: Map[String, Preset] = ???
  def runApp(appName: String): Unit = appHM.get(appName)
    .map(_.run())
    .getOrElse{
      throw new InvalidClassException(s"Preset app ${appName} is not recognised.")
    }
}

sealed trait Preset {
  def run(): Unit
}

case class Record(id: Int, name: String, scores: Array[Double])

private final case object DataGenerator extends DataLoader[Iterable[Record]] {
  override def run(input: String): Iterable[Record] =
    Seq(
      Record(1, "foo", Array.empty),
      Record(2, "baz", Array(1.3, 2.5, 3.3)),
      Record(3, "aaa", Array(0.1)),
      Record(4, "azz", Array(2.5, 3.36, 2.001))
    )
}

final class DataSerialiser extends Preset {
  private val workflow = DataGenerator :: ??? :: Nil // todo:

  override def run(): Unit = ???
}
