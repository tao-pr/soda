package de.tao.soda.etl.data

import de.tao.soda.etl.{DataLoader, Workflow}


object DB {
  trait Secret {
    def getUser(key: String): Option[String] = sys.env.get(s"user_$key")
    def getPwd(key: String): Option[String] = sys.env.get(s"pwd_$key")
  }
  abstract class ConnectionConfig extends Secret
  case class MySqlConfig(host: String, port: Int, db: String, table: String) extends ConnectionConfig
  case class RedisConfig(host: String, port: Int, db: Int=0) extends ConnectionConfig
  class H2Config extends ConnectionConfig
  class SqliteConfig extends ConnectionConfig

  def caseClassToMap(instance: AnyRef): Map[String, Any] = {
    instance.getClass.getDeclaredFields.map { f =>
      f.setAccessible(true)
      (f.getName -> f.get(instance))
    }.filterNot(_._1.contains('$')).toMap
  }
}

trait ReadFromDB[T] extends DataLoader[Iterable[T]]{
  def read(query: Map[String, Any]): Iterable[T]

  val config: DB.ConnectionConfig
  def query: Map[String, AnyVal]

  override def run(input: String): Iterable[T] = {
    logger.info(s"${this.getClass.getName} reading from storage: $config")
    read(query)
  }
}

trait ReadIteratorFromDB[T] extends DataLoader[Iterator[T]]{
  def read(query: Map[String, Any]): Iterator[T]

  val config: DB.ConnectionConfig
  def query: Map[String, AnyVal]

  override def run(input: String): Iterator[T] = {
    logger.info(s"${this.getClass.getName} reading iterator from storage: $config")
    read(query)
  }
}

trait WriteToDB[T] extends Workflow[Iterable[T], Iterable[T]]{
  def write(data: Iterable[T]): Iterable[T]

  val config: DB.ConnectionConfig

  override def run(input: Iterable[T]): Iterable[T] = {
    logger.info(s"${this.getClass.getName} writing to storage: $config")
    write(input)
    input
  }
}


