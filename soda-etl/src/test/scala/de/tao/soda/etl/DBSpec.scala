package de.tao.soda.etl

import de.tao.soda.etl.Domain.MySqlFoo
import de.tao.soda.etl.data.DB
import de.tao.soda.etl.data.db.{ReadFromMySql, WriteToMySql}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.ResultSet

object MySqlUtil {
  def parser(rs: ResultSet): MySqlFoo = {
    MySqlFoo(rs.getString("uuid"), rs.getString("name"), rs.getLong("code"), rs.getDouble("baz"))
  }
}

class DBSpec extends AnyFlatSpec with BeforeAndAfterAll {

  lazy val mysqlConfig = DB.MySqlConfig("localhost", 3306, "test", "foo")
  lazy val mysqlSecret = DB.ParamSecret("test", "testpwd")

  it should "read from mysql" in {
    lazy val query = Map[String, Any]("name" -> "='melon'")
    lazy val mysqlRead = new ReadFromMySql[MySqlFoo](mysqlConfig, mysqlSecret, MySqlUtil.parser)

    val out = mysqlRead.run(query)
    assert(out.size == 1)
    assert(out.head.uuid == "9a02de5b-9449-466d-9269-ba798e7b56dd")
    assert(out.head.name == "Melon")
    assert(out.head.code == 15)
    assert(out.head.baz == 2.55551)
  }

  it should "execute update mysql" in {
    val sqlClean = "DELETE FROM foo WHERE name LIKE 'name-%'"
    lazy val mysqlWrite = new WriteToMySql[MySqlFoo](mysqlConfig, mysqlSecret, Some(sqlClean))
    val records = List(
      MySqlFoo(java.util.UUID.randomUUID().toString, "name-1", 15, -1.3e6),
      MySqlFoo(java.util.UUID.randomUUID().toString, "name-2", 1, -1.3e6),
      MySqlFoo(java.util.UUID.randomUUID().toString, "name-3", 4294967295L, Double.MinValue),
      MySqlFoo(java.util.UUID.randomUUID().toString, "name-4", 4294967295L, Double.MaxValue),
      MySqlFoo(java.util.UUID.randomUUID().toString, "name-5", 0, 20e4)
    )
    val ns = mysqlWrite.run(records)

    Thread.sleep(1000)

    // test query back
    lazy val mysqlRead = new ReadFromMySql[MySqlFoo](mysqlConfig, mysqlSecret, MySqlUtil.parser)
    val out = mysqlRead.run(Map[String, Any]("name" -> "LIKE 'name-%'"))

    assert(out.size == records.size)
  }

  it should "read from redis" in {
    ???
  }

}
