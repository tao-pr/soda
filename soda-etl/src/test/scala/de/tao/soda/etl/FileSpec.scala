package de.tao.soda.etl

import de.tao.soda.etl.Implicits._
import de.tao.soda.etl.data._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import Domain._
import de.tao.soda.etl.data.OutputIdentifier.$

class FileSpec extends AnyFlatSpec with BeforeAndAfter {

  lazy val csvTest = scala.io.Source.fromResource("data.csv").lift
  lazy val jsonTest = scala.io.Source.fromResource("data.json").lift

  it should "read CSV file" in {

    val csv = ReadCSV[CSVData](',').run(csvTest)
    assert(csv.isInstanceOf[Iterator[CSVData]])

    val csvList = csv.toList
    assert(csvList.size == 6)
    assert(csvList.map(_.name).sorted == List("David Brown", "Gyle Roland", "Jason Bread", "Joe Grass", "Keleb Dean", "Marcus Mooy"))
  }

  it should "read JSON file" in {

    implicit val csvDataClass = classOf[JSONData]
    val json = new ReadJSON[JSONData].run(jsonTest)

    assert(json.isInstanceOf[JSONData])
    assert(json.header.p.isEmpty)
    assert(json.header.title == "foobar")
    assert(json.body.s == List(0,0,1,0,5))
    assert(json.b == true)
  }

  it should "write an object to file" in {
    val src = JSONList(Array.fill(5100)(JSONData(H1("title", scala.util.Random.nextInt(), Some("thing")), B1(List.fill(100)(1)), false)))

    // serialise
    val tempFile = java.io.File.createTempFile("sodatest", "jsondata")
    val serialiser = WriteAsObject[JSONList]($(tempFile.getAbsolutePath))
    serialiser.run(src)

    // deserialiser
    val deserialiser = new ReadAsObjectOpt[JSONList]
    val destOpt = deserialiser.run(PathIdentifier(tempFile.getAbsolutePath))
    assert(destOpt.isDefined)
    assert(destOpt.get.arr.size == 5100)
    assert(destOpt.get.arr.head.header.title == "title")
    assert(destOpt.get.arr.head.header.p.contains("thing"))
    assert(!destOpt.get.arr.head.b)
    assert(destOpt.get.arr.head.body.s.size == 100)

    tempFile.delete()
  }

  it should "write an object to a zip file" in {
    val src = JSONList(Array.fill(5100)(JSONData(H1("title", scala.util.Random.nextInt(), Some("thing")), B1(List.fill(100)(1)), false)))

    // serialise
    val tempFile = java.io.File.createTempFile("sodatest", "jsonzipped")
    val serialiser = WriteAsZippedObject[JSONList](OutputPath(tempFile.getAbsolutePath))
    serialiser.run(src)

    // deserialiser
    val deserialiser = new ReadZippedAsObject[JSONList]
    val destOpt = deserialiser.run(PathIdentifier(tempFile.getAbsolutePath))
    assert(destOpt.arr.size == 5100)
    assert(destOpt.arr.head.header.title == "title")
    assert(destOpt.arr.head.header.p.contains("thing"))
    assert(!destOpt.arr.head.b)
    assert(destOpt.arr.head.body.s.size == 100)

    tempFile.delete()
  }
}
