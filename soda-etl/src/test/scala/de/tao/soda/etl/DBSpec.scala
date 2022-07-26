package de.tao.soda.etl

import de.tao.soda.etl.data.db.ReadFromMySql
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.flatspec.AnyFlatSpec
import de.tao.soda.etl.data.DB
import Domain._

import java.sql.ResultSet

object MySqlUtil {
  def parser(rs: ResultSet): MySqlFoo = {
    MySqlFoo(rs.getString("uuid"), rs.getString("name"), rs.getInt("code"), rs.getDouble("baz"))
  }
}

class DBSpec extends AnyFlatSpec with BeforeAndAfterAll {

  lazy val mysqlConfig = new DB.MySqlConfig("localhost", 3306, "test", "foo")
  lazy val mysqlSecret = new DB.ParamSecret("test", "testpwd")

  it should "read from mysql" in {
    lazy val query = Map.empty[String, AnyVal]
    lazy val mysqlRead = new ReadFromMySql[MySqlFoo](mysqlConfig, mysqlSecret, MySqlUtil.parser)

    val out = mysqlRead.run(query)
    assert(out.size == 1)
  }

}
