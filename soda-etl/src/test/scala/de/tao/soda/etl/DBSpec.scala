package de.tao.soda.etl

import com.redis.serialization.Parse
import de.tao.soda.etl.Domain.{MySqlFoo, RedisFoo}
import de.tao.soda.etl.data.DB
import de.tao.soda.etl.data.db.{FlushRedis, ReadFromMySql, ReadFromRedis, WriteToMySql, WriteToRedis}
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

  lazy val redisConfig = DB.RedisConfig("localhost", 6379, db=0)
  lazy val redisSecret = DB.ParamPwdSecret("testpwd")

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

  it should "write and read from redis" in {

    // flush redis before writing
    new FlushRedis[Nil.type](redisConfig, redisSecret).run(Nil)

    lazy val redisWrite = new WriteToRedis[RedisFoo](redisConfig, redisSecret)
    val records = List(
      // key -> field -> value
      ("key1", "field1", RedisFoo(java.util.UUID.randomUUID().toString, "name1", 100, Array.empty)),
      ("key1", "field2", RedisFoo(java.util.UUID.randomUUID().toString, "name2", 200, Array(1,2,3))),
      ("key2", "field1", RedisFoo(java.util.UUID.randomUUID().toString, "name3", 200, Array(1,2,3))),
      ("key2", "field2", RedisFoo(java.util.UUID.randomUUID().toString, "name2", 300, Array(1,2,3,4,5))),
      ("key2", "field3", RedisFoo(java.util.UUID.randomUUID().toString, "name3", 200, Array(1)))
    )

    redisWrite.run(records)

    Thread.sleep(200)

    // parse as json string
    implicit val parser = new Parse[RedisFoo](f = RedisFoo.fromBytes)

    // test query back
    lazy val redisRead = new ReadFromRedis[RedisFoo](redisConfig, redisSecret)
    val queryMap = Map("key1" -> List("field1","field2"), "key2" -> List("field1", "field2", "field3"))
    val out = redisRead.run(queryMap).toList

    assert(out.size == records.size)

    val outStr = out.map{ case (k,v,f) => s"$k, $v, $f"}
    val expStr = records.map{ case (k,v,f) => s"$k, $v, $f"}
    assert(expStr.forall(outStr.contains(_)))
  }

}
