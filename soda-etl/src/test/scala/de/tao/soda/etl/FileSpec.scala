package de.tao.soda.etl

import de.tao.soda.etl.data.CSVFileReader
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.io.InputStreamReader
import scala.io.BufferedSource
import de.tao.soda.etl.Implicits._

case class CSVData(id: Int, name: String, occupation: String, subscribed: Boolean, score: Int)

class FileSpec extends AnyFlatSpec with BeforeAndAfter {

  lazy val csvTest = scala.io.Source.fromResource("data.csv").lift
  lazy val jsonTest = scala.io.Source.fromResource("data.json").lift

  it should "read CSV file" in {

    val csv = CSVFileReader[CSVData](',').run(csvTest)
    assert(csv.isInstanceOf[Iterator[CSVData]])

    val csvList = csv.toList
    assert(csvList.size == 6)
    assert(csvList.map(_.name).toList.sorted == List("David Brown", "Gyle Roland", "Jason Bread", "Joe Grass", "Keleb Dean", "Marcus Mooy"))
  }

}
