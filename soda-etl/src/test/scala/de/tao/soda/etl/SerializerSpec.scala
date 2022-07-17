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
    val json = new ReadJSON[JSONData].run(jsonTest)

    // serialise
    val bytes = new SerializeJSON[JSONData].run(json)
    assert(bytes.nonEmpty)

    // deserialise
    val djson = new DeserializeJSON[JSONData].run(bytes)
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

  it should "compress UTF-8 string" in {
    val str = "{hey @somedude @straßer, have you got a chocolate?}"
    val bytes = str.getBytes("UTF-8")
    val cbytes = new GzipCompress().run(bytes)
    val dbytes = new GzipDecompress().run(cbytes)
    val dstr = new String(dbytes, "UTF-8")

    assert(str.size == dstr.size)
    assert(str == dstr)
  }

}
