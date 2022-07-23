package de.tao.soda.etl.data.db

import de.tao.soda.etl.data.{DB, ReadFromDB, ReadIteratorFromDB, WriteToDB}

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

trait MySql {
  val Key = "mysql"

  def connection(config: DB.MySqlConfig): Connection = {
    val url = s"jdbc:mysql://${config.host}:${config.port}/mysql"
    DriverManager.getConnection(url, config.getUser(Key).getOrElse(""), config.getPwd(Key).getOrElse(""))
  }

  var conn: Option[Connection] = None

  protected def makeSelect[T](query: Map[String, Any], config: DB.MySqlConfig): (Statement, String) = {
    val cond = query.map { case (k, v) => s"$k $v" }.mkString(" and ")
    conn = Some(connection(config))
    val smt = conn.get.createStatement()
    (smt, s"SELECT * FROM ${config.table} WHERE ${cond}")
  }
}

class ReadFromMySql[T](override val config: DB.MySqlConfig, override val query: Map[String, AnyVal], parser: (ResultSet => T)) extends ReadFromDB[T] with MySql {

  override def read(query: Map[String, Any]): Iterable[T] = {

    val (smt, sql) = makeSelect[T](query, config)
    val rs = smt.executeQuery(sql)
    val arr = new ArrayBuffer[T]
    while (rs.next()){
      arr += parser(rs)
    }
    arr
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromMySql: tearing down connection")
    conn.map(_.close())
  }
}

final class MySqlRecordIterator[T](rs: ResultSet, parser: (ResultSet => T)) extends Iterator[T]{
  override def hasNext: Boolean = !rs.last()

  override def next(): T = {
    rs.next()
    parser(rs)
  }
}

class ReadIteratorFromMySql[T](override val config: DB.MySqlConfig, override val query: Map[String, AnyVal], parser: (ResultSet => T)) extends ReadIteratorFromDB[T] with MySql {

  override def read(query: Map[String, Any]): Iterator[T] = {
    val (smt, sql) = makeSelect[T](query, config)
    val rs = smt.executeQuery(sql)
    new MySqlRecordIterator[T](rs, parser)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadIteratorFromMySql: tearing down connection")
    conn.map(_.close())
  }
}

class WriteToMySql[T <: AnyRef](override val config: DB.MySqlConfig, prewriteSql: Option[String]=None) extends WriteToDB[T] with MySql {

  override def write(data: Iterable[T]): Iterable[T] = {
    val conn = connection(config)
    val smt = conn.createStatement()
    prewriteSql.map{ sql =>
      logger.info("WriteToMySql : Executing pre-write sql")
      smt.executeUpdate(sql)
    }
    val fieldMap = DB.caseClassToMap(data.head)
    val valueMap = fieldMap.map{ case (_,v) => if (v.isInstanceOf[String]) s"'$v'" else v.toString }
    val sql = s"INSERT INTO ${config.table} (${fieldMap.keys.mkString(",")}) VALUES ($valueMap)"
    val n = smt.executeUpdate(sql)
    logger.info(s"WriteToMySql : written $n rows")
    data
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadIteratorFromMySql: tearing down connection")
    conn.map(_.close())
  }
}
