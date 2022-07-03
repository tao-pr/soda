package de.tao.soda.etl

import Domain._
import de.tao.soda.etl.Implicits._
import de.tao.soda.etl.data._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class SerializerSpec extends AnyFlatSpec with BeforeAndAfter {

  lazy val jsonTest = scala.io.Source.fromResource("data.json").lift

  it should "serialise and deserialise JSON" in {
    implicit val jsonDataClass = classOf[JSONData]
    val json = new JSONReader[JSONData].run(jsonTest)

    // serialise
    val bytes = new JSONSerializer[JSONData].run(json)
    assert(bytes.nonEmpty)

    // deserialise
    val djson = new JSONDeserializer[JSONData].run(bytes)
    assert(djson.isInstanceOf[JSONData])
    assert(djson == json)
  }

  it should "compress and decompress byte array" in {

    val bytes = (1 to 6506).map(a => (a%255).toByte).toArray
    val cbytes = new GzipCompress().run(bytes)
    val dbytes = new GzipDecompress().run(cbytes)

    assert(cbytes.size < bytes.size)
    assert(dbytes.size == bytes.size)
    assert(dbytes.map(_.toInt).diff(bytes.map(_.toInt)).isEmpty)
  }

}
