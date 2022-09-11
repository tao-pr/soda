package de.tao.soda.etl.data.db

import de.tao.soda.etl.data.DB.H2Config
import de.tao.soda.etl.data.{DB, ReadFromDB, ReadIteratorFromDB, WriteToDB}

import java.sql.ResultSet
import de.tao.soda.etl.data.Filter


class ReadFromH2[T](
override val config: H2Config,
override val secret: DB.Secret,
parser: (ResultSet => T))
extends ReadFromDB[T] with Jdbc {

  override def read(query: Filter): Iterable[T] = {
    readTable[T](query, config, secret, parser)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromH2: tearing down connection")
    conn.map(_.close())
  }
}

class ReadIteratorFromH2[T](
override val config: H2Config,
override val secret: DB.Secret,
parser: (ResultSet => T))
  extends ReadIteratorFromDB[T] with Jdbc {

  override def read(query: Filter): Iterator[T] = {
    readTableAsIterator[T](query, config, secret, parser)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadIteratorFromH2: tearing down connection")
    conn.map(_.close())
  }
}


class WriteToH2[T <: AnyRef](
override val config: DB.H2Config,
override val secret: DB.Secret,
prewriteSql: Option[String]=None) extends WriteToDB[T] with Jdbc {

  override def write(data: Iterable[T]): Iterable[T] = {
    val n = writeTable(data, config, secret, prewriteSql)
    if (n>=0)
      logger.info(s"WriteToH2 : writing ${n} records")
    else
      logger.warn(s"WriteToH2 : will not write any records, empty input")

    data
  }

  override def shutdownHook(): Unit = {
    logger.info(s"WriteToH2: tearing down connection")
    conn.map(_.close())
  }
}
