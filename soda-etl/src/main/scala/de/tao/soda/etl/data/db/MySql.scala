package de.tao.soda.etl.data.db

import de.tao.soda.etl.data.{DB, ReadFromDB, ReadIteratorFromDB, WriteToDB}

import java.sql.ResultSet
import de.tao.soda.etl.data.Filter


class ReadFromMySql[T](
override val config: DB.MySqlConfig,
override val secret: DB.Secret,
parser: (ResultSet => T)) extends ReadFromDB[T] with Jdbc {

  override def read(query: Filter): Iterable[T] = {
    readTable[T](query, config, secret, parser)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromMySql: tearing down connection")
    conn.map(_.close())
  }
}

class ReadIteratorFromMySql[T](
override val config: DB.MySqlConfig,
override val secret: DB.Secret,
parser: (ResultSet => T)) extends ReadIteratorFromDB[T] with Jdbc {

  override def read(query: Filter): Iterator[T] = {
    readTableAsIterator(query, config, secret, parser)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadIteratorFromMySql: tearing down connection")
    conn.map(_.close())
  }
}

class WriteToMySql[T <: AnyRef](
override val config: DB.MySqlConfig,
override val secret: DB.Secret,
prewriteSql: Option[String]=None) extends WriteToDB[T] with Jdbc {

  override def write(data: Iterable[T]): Iterable[T] = {
    val n = writeTable(data, config, secret, prewriteSql)
    if (n>=0)
      logger.info(s"WriteToMySql : writing ${n} records")
    else
      logger.warn(s"WriteToMySql : will not write any records, empty input")

    data
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadIteratorFromMySql: tearing down connection")
    conn.map(_.close())
  }
}
