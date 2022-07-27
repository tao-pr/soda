package de.tao.soda.etl.data.db

import de.tao.soda.etl.data.{DB, ReadFromDB, ReadIteratorFromDB, WriteToDB}

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer

trait MySql {

  def connection(config: DB.MySqlConfig, secret: DB.Secret): Connection = {
    val url = s"jdbc:mysql://${config.host}:${config.port}/${config.db}"
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection(url, secret.getUser.getOrElse(""), secret.getPwd.getOrElse(""))
  }

  var conn: Option[Connection] = None

  protected def makeSelect[T](query: Map[String, Any], config: DB.MySqlConfig, secret: DB.Secret): (Statement, String) = {
    val cond = if (query.isEmpty) ""
    else " WHERE " + query.map { case (k, v) => s"`$k` $v" }.mkString(" and ")

    conn = Some(connection(config, secret))
    val smt = conn.get.createStatement()
    (smt, s"SELECT * FROM ${config.table} ${cond}")
  }
}

class ReadFromMySql[T](
override val config: DB.MySqlConfig,
override val secret: DB.Secret,
parser: (ResultSet => T)) extends ReadFromDB[T] with MySql {

  override def read(query: Map[String, Any]): Iterable[T] = {

    val (smt, sql) = makeSelect[T](query, config, secret)
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

class ReadIteratorFromMySql[T](
override val config: DB.MySqlConfig,
override val secret: DB.Secret,
parser: (ResultSet => T)) extends ReadIteratorFromDB[T] with MySql {

  override def read(query: Map[String, Any]): Iterator[T] = {
    val (smt, sql) = makeSelect[T](query, config, secret)
    val rs = smt.executeQuery(sql)
    new MySqlRecordIterator[T](rs, parser)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadIteratorFromMySql: tearing down connection")
    conn.map(_.close())
  }
}

class WriteToMySql[T <: AnyRef](
override val config: DB.MySqlConfig,
override val secret: DB.Secret,
prewriteSql: Option[String]=None) extends WriteToDB[T] with MySql {

  override def write(data: Iterable[T]): Iterable[T] = {
    val conn = connection(config, secret)
    val smt = conn.createStatement()
    prewriteSql.map{ sql =>
      logger.info("WriteToMySql : Executing pre-write sql")
      smt.executeUpdate(sql)
    }

    if (data.nonEmpty) {
      val num = data.map{ rec =>
        val fieldMap = DB.caseClassToMap(rec)
        val valueMap = fieldMap.map { case (_, v) => if (v.isInstanceOf[String]) s"'$v'" else v.toString }
        val sql = s"INSERT INTO `${config.table}` (${fieldMap.keys.map(k => s"`${k}`").mkString(",")}) VALUES (${valueMap.mkString(",")})"
        logger.info(sql) // todo:
        smt.executeUpdate(sql)
      }.sum

      logger.info(s"WriteToMySql : written $num rows")
      data
    }
    else {
      logger.info(s"WriteToMySql: skip writing collection to DB, input is empty")
      data
    }
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadIteratorFromMySql: tearing down connection")
    conn.map(_.close())
  }
}
