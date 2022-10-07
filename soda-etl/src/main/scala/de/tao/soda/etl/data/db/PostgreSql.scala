package de.tao.soda.etl.data.db

import de.tao.soda.etl.data.DB.PostgreSqlConfig
import de.tao.soda.etl.data.{DB, ReadFromDB, ReadIteratorFromDB, WriteToDB}

import java.sql.ResultSet
import de.tao.soda.etl.data.Filter

class ReadFromPosgreSql[T](
override val config: PostgreSqlConfig,
override val secret: DB.Secret,
parser: (ResultSet => T))
  extends ReadFromDB[T] with Jdbc {

  override def read(query: Filter): Iterable[T] = {
    readTable[T](query, config, secret, parser)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromPosgreSql: tearing down connection")
    conn.map(_.close())
  }
}

class ReadIteratorFromPosgreSql[T](
override val config: PostgreSqlConfig,
override val secret: DB.Secret,
parser: (ResultSet => T))
  extends ReadIteratorFromDB[T] with Jdbc {

  override def read(query: Filter): Iterator[T] = {
    readTableAsIterator[T](query, config, secret, parser)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadIteratorFromPosgreSql: tearing down connection")
    conn.map(_.close())
  }
}


class WriteToPosgreSql[T <: AnyRef](
override val config: DB.PostgreSqlConfig,
override val secret: DB.Secret,
prewriteSql: Option[String]=None) extends WriteToDB[T] with Jdbc {

  override def write(data: Iterable[T]): Iterable[T] = {
    val n = writeTable(data, config, secret, prewriteSql)
    if (n>=0)
      logger.info(s"WriteToPosgreSql : writing ${n} records")
    else
      logger.warn(s"WriteToPosgreSql : will not write any records, empty input")

    data
  }

  override def shutdownHook(): Unit = {
    logger.info(s"WriteToPosgreSql: tearing down connection")
    conn.map(_.close())
  }
}
