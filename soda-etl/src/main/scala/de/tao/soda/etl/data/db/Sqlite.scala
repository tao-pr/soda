package de.tao.soda.etl.data.db

import de.tao.soda.etl.data.DB.SqliteConfig
import de.tao.soda.etl.data.{DB, ReadFromDB, ReadIteratorFromDB, WriteToDB}
import de.tao.soda.etl.data._

import java.sql.ResultSet

class ReadFromSqlite[T](
override val config: SqliteConfig,
override val secret: DB.Secret,
parser: (ResultSet => T))
  extends ReadFromDB[T] with Jdbc {

  override def read(query: Filter): Iterable[T] = {
    readTable[T](query, config, secret, parser)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromSqlite: tearing down connection")
    conn.map(_.close())
  }
}

// taotodo: read iterator


class WriteToSqlite[T <: AnyRef](
override val config: DB.SqliteConfig,
override val secret: DB.Secret,
prewriteSql: Option[String]=None) extends WriteToDB[T] with Jdbc {

  override def write(data: Iterable[T]): Iterable[T] = {
    val n = writeTable(data, config, secret, prewriteSql)
    if (n>=0)
      logger.info(s"WriteToSqlite : writing ${n} records")
    else
      logger.warn(s"WriteToSqlite : will not write any records, empty input")

    data
  }

  override def shutdownHook(): Unit = {
    logger.info(s"WriteToSqlite: tearing down connection")
    conn.map(_.close())
  }
}