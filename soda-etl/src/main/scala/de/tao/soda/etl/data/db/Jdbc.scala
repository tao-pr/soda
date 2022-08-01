package de.tao.soda.etl.data.db

import de.tao.soda.etl.data.DB

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer

trait Jdbc {
  def connection(config: DB.JdbcConnectionConfig, secret: DB.Secret): Connection = {
    Class.forName(config.className)
    DriverManager.getConnection(config.url, secret.getUser.getOrElse(""), secret.getPwd.getOrElse(""))
  }

  protected var conn: Option[Connection] = None

  protected def makeSelect[T](query: Map[String, Any], config: DB.JdbcConnectionConfig, secret: DB.Secret): (Statement, String) = {
    val cond = if (query.isEmpty) ""
    else " WHERE " + query.map { case (k, v) => s"`$k` $v" }.mkString(" and ")

    if (conn.isEmpty)
      conn = Some(connection(config, secret))
    val smt = conn.get.createStatement()
    (smt, s"SELECT * FROM ${config.table} ${cond}")
  }

  protected def readTable[T](query: Map[String, Any], config: DB.JdbcConnectionConfig, secret: DB.Secret, parser: (ResultSet => T)): Iterable[T] = {
    val (smt, sql) = makeSelect[T](query, config, secret)
    val rs = smt.executeQuery(sql)
    val arr = new ArrayBuffer[T]
    while (rs.next()){
      arr += parser(rs)
    }
    arr
  }

  protected def readTableAsIterator[T](query: Map[String, Any], config: DB.JdbcConnectionConfig, secret: DB.Secret, parser: (ResultSet => T)): Iterator[T] = {
    val (smt, sql) = makeSelect[T](query, config, secret)
    val rs = smt.executeQuery(sql)
    new JdbcRecordIterator[T](rs, parser)
  }

  protected def writeTable[T <: AnyRef](data: Iterable[T],config: DB.JdbcConnectionConfig, secret: DB.Secret, prewriteSql: Option[String]=None): Int = {
    if (conn.isEmpty)
      conn = Some(connection(config, secret))
    val smt = conn.get.createStatement()
    prewriteSql.map{ sql =>
      smt.executeUpdate(sql)
    }

    if (data.nonEmpty) {
      val num = data.map{ rec =>
        val fieldMap = DB.caseClassToMap(rec)
        // todo: use statement value instead
        val valueMap = fieldMap.map { case (_, v) => if (v.isInstanceOf[String]) s"'$v'" else v.toString }
        val sql = s"INSERT INTO `${config.table}` (${fieldMap.keys.map(k => s"`${k}`").mkString(",")}) VALUES (${valueMap.mkString(",")})"
        smt.executeUpdate(sql)
      }.sum

      num
    }
    else -1
  }

  // todo: delete table

}

class JdbcRecordIterator[T](rs: ResultSet, parser: (ResultSet => T)) extends Iterator[T]{
  override def hasNext: Boolean = !rs.last()

  override def next(): T = {
    rs.next()
    parser(rs)
  }
}

