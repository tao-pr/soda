package de.tao.soda.etl

import com.redis.serialization.Parse
import de.tao.soda.etl.Domain.{H2Foo, MySqlFoo, PostgresFoo, RedisFoo}
import de.tao.soda.etl.data.DB
import de.tao.soda.etl.data.db._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.ResultSet

object MySqlUtil {
  def parser(rs: ResultSet): MySqlFoo = {
    MySqlFoo(rs.getString("uuid"), rs.getString("name"), rs.getLong("code"), rs.getDouble("baz"))
  }
}

object H2Util {
  def parser(rs: ResultSet): H2Foo = {
    H2Foo(rs.getString("uuid"), rs.getInt("i"), rs.getDouble("d"))
  }
}

object PostgresUtil {
  def parser(rs: ResultSet): PostgresFoo = {
    PostgresFoo(rs.getString("uuid"), rs.getString("s"), rs.getDouble("d"))
  }
}


class DBSpec extends AnyFlatSpec with BeforeAndAfterAll {

  lazy val mysqlConfig = DB.MySqlConfig("localhost", 3306, "test", "foo")
  lazy val mysqlSecret = DB.ParamSecret("test", "testpwd")

  lazy val redisConfig = DB.RedisConfig("localhost", 6379, db=0)
  lazy val redisSecret = DB.ParamPwdSecret("testpwd")

  lazy val h2Config = DB.H2Config("./", "soda-h2-test", "tb1")
  lazy val h2Secret = DB.ParamPwdSecret("testpwd")

  lazy val postgresConfig = DB.PostgreSqlConfig("localhost", 5432, "test", "tbtest")
  lazy val postgresSecret = DB.ParamSecret("thetest", "testpwd")

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

  it should "read an iterator from mysql" in {
    lazy val mysqlRead = new ReadIteratorFromMySql[MySqlFoo](mysqlConfig, mysqlSecret, MySqlUtil.parser)
    val iter = mysqlRead.run(Map[String, Any]("name" -> "LIKE 'name-%'"))

    assert(iter.isInstanceOf[Iterator[_]])
    var nameSet = (1 to 5).map(s => s"name-$s").toSet
    for (n <- iter){
      assert(nameSet.contains(n.name))
      nameSet = nameSet.excl(n.name)
    }
  }

  it should "write and read from redis" in {

    // flush redis before writing
    new FlushRedis[Nil.type](redisConfig, redisSecret).run(Nil)

    val expireF = (key: String) => if (key=="key1") Some(2) else None
    lazy val redisWrite = new WriteToRedis[RedisFoo](redisConfig, redisSecret, expireF)
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

    // test expire
    Thread.sleep(2500)
    assert(redisRead.run(queryMap).toList.count(_._1 == "key1")==0)
  }

  it should "handle non-existing key or field in redis when reading" in {
    // parse as json string
    implicit val parser = new Parse[RedisFoo](f = RedisFoo.fromBytes)

    lazy val redisRead = new ReadFromRedis[RedisFoo](redisConfig, redisSecret)
    val queryMap = Map("key1" -> List("field1","field2"), "key2" -> List("field1", "field2", "field3", "notexist", "key3" -> List("notexist")))
    val out = redisRead.run(queryMap).toList

    assert(out.size == 3) // NOTE: key1 already expired earlier
  }

  it should "write and read from H2" in {
    val sqlCreateTb =
      """
        |DROP TABLE tb1 IF EXISTS;
        |CREATE TABLE tb1(
        |UUID VARCHAR(36), i INT, d FLOAT);
        |""".stripMargin
    lazy val h2write = new WriteToH2[H2Foo](h2Config, h2Secret, Some(sqlCreateTb))
    val records = List(
      H2Foo(java.util.UUID.randomUUID().toString, 1, 1e-3),
      H2Foo(java.util.UUID.randomUUID().toString, 2, 1e-6),
      H2Foo(java.util.UUID.randomUUID().toString, 3, 1e-12),
      H2Foo(java.util.UUID.randomUUID().toString, 4, 1e24)
    )

    val ns = h2write.run(records)

    assert(ns == records)

    // try reading back
    lazy val h2Read = new ReadFromH2[H2Foo](h2Config, h2Secret, H2Util.parser)
    val out = h2Read.run(Map("i" -> ">0"))

    assert(out.size == records.size)
    assert(out.map(_.uuid).toList.sorted == records.map(_.uuid).sorted)
  }

  it should "read an iterator from h2" in {
    lazy val h2Read = new ReadIteratorFromH2[H2Foo](h2Config, mysqlSecret, H2Util.parser)
    val iter = h2Read.run(Map[String, Any]("i" -> ">0"))

    assert(iter.isInstanceOf[Iterator[_]])
    var idSet = (1 to 5).toSet
    for (n <- iter){
      assert(idSet.contains(n.i))
      idSet = idSet.excl(n.i)
    }
  }

  it should "write and read from postgres" in {
    val sqlCreateTb =
      """
        |DROP TABLE IF EXISTS tbtest;
        |CREATE TABLE tbtest(
        |UUID VARCHAR(36), s VARCHAR(1), d FLOAT);
        |""".stripMargin
    lazy val postwrite = new WriteToPosgreSql[PostgresFoo](postgresConfig, postgresSecret, Some(sqlCreateTb))
    val records = List(
      PostgresFoo(java.util.UUID.randomUUID().toString, "1", 1e-3),
      PostgresFoo(java.util.UUID.randomUUID().toString, "2", 1e-6),
      PostgresFoo(java.util.UUID.randomUUID().toString, "3", 1e-12)
    )

    val ns = postwrite.run(records)

    assert(ns == records)

    // try reading back
    lazy val postRead = new ReadFromPosgreSql[PostgresFoo](postgresConfig, postgresSecret, PostgresUtil.parser)
    val out = postRead.run(Map("d" -> ">0"))

    assert(out.size == records.size)
    assert(out.map(_.uuid).toList.sorted == records.map(_.uuid).sorted)
  }

}
