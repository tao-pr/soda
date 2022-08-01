package de.tao.soda.etl

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.ByteArrayOutputStream

object Domain {
  // For FileSpec, SerializerSpec, WorkflowSpec, WorkSequenceSpec
  case class CSVData(id: Int, name: String, occupation: String, subscribed: Boolean, score: Int)
  case class H1(title: String, id: Int, p: Option[String])
  case class B1(s: List[Int])
  case class JSONData(header: H1, body: B1, b: Boolean)
  case class JSONList(arr: Array[JSONData])

  // For DBSpec
  case class MySqlFoo(uuid: String, name: String, code: Long, baz: Double)
  case class RedisFoo(uuid: String, name: String, code: Long, arr: Array[Int]){
    override def toString: String = {
      val mapper = JsonMapper.builder()
        .addModule(DefaultScalaModule)
        .build()
      val bstream = new ByteArrayOutputStream()
      mapper.writeValue(bstream, this)
      bstream.toString("utf-8")
    }
  }

  object RedisFoo {
    def fromBytes(bytes: Array[Byte]): RedisFoo = {
      val mapper: JsonMapper = JsonMapper.builder()
        .addModule(DefaultScalaModule)
        .build()
      val str = new String(bytes, "utf-8")
      mapper.readValue(str, classOf[RedisFoo])
    }
  }

  case class H2Foo(uuid: String, i: Int, d: Double)
  case class PostgresFoo(uuid: String, s: String, d: Double)
}
