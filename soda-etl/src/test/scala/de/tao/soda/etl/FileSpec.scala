package de.tao.soda.etl

import de.tao.soda.etl.data.{CSVFileReader, JSONReader}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.io.InputStreamReader
import scala.io.BufferedSource
import de.tao.soda.etl.Implicits._

case class CSVData(id: Int, name: String, occupation: String, subscribed: Boolean, score: Int)
case class H1(title: String, id: Int, p: Option[String])
case class B1(s: List[Int])
case class JSONData(header: H1, body: B1, b: Boolean)

class FileSpec extends AnyFlatSpec with BeforeAndAfter {

  lazy val csvTest = scala.io.Source.fromResource("data.csv").lift
  lazy val jsonTest = scala.io.Source.fromResource("data.json").lift

  it should "read CSV file" in {

    val csv = CSVFileReader[CSVData](',').run(csvTest)
    assert(csv.isInstanceOf[Iterator[CSVData]])

    val csvList = csv.toList
    assert(csvList.size == 6)
    assert(csvList.map(_.name).sorted == List("David Brown", "Gyle Roland", "Jason Bread", "Joe Grass", "Keleb Dean", "Marcus Mooy"))
  }

  it should "read JSON file" in {

    implicit val csvDataClass = classOf[JSONData]
    val json = new JSONReader[JSONData].run(jsonTest)

    assert(json.isInstanceOf[Option[JSONData]])
    assert(!json.isEmpty)
    assert(json.get.header.p.isEmpty)
    assert(json.get.header.title == "foobar")
    assert(json.get.body.s == List(0,0,1,0,5))
    assert(json.get.b == true)
  }
}
