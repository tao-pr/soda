package de.tao.soda.etl

object Domain {
  // For FileSpec, SerializerSpec, WorkflowSpec, WorkSequenceSpec
  case class CSVData(id: Int, name: String, occupation: String, subscribed: Boolean, score: Int)
  case class H1(title: String, id: Int, p: Option[String])
  case class B1(s: List[Int])
  case class JSONData(header: H1, body: B1, b: Boolean)
  case class JSONList(arr: Array[JSONData])

  // For DBSpec
  case class MySqlFoo(uuid: String, name: String, code: Long, baz: Double)

}
