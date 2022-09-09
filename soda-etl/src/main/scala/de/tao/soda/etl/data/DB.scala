package de.tao.soda.etl.data

import de.tao.soda.etl.{DataQuery, Workflow}
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients


object DB {
  trait Secret {
    def getUser: Option[String]
    def getPwd: Option[String]
  }

  case class EnvSecret(key: String) extends Secret {
    override def getUser: Option[String] = sys.env.get(s"user_$key")
    override def getPwd: Option[String] = sys.env.get(s"pwd_$key")
  }

  case class ParamSecret(usr: String, pwd: String) extends Secret {
    override def getUser: Option[String] = Some(usr)
    override def getPwd: Option[String] = Some(pwd)
  }

  case class ParamPwdSecret(pwd: String) extends Secret {
    override def getUser: Option[String] = None
    override def getPwd: Option[String] = Some(pwd)
  }

  case class DotFileSecret(key: String) extends Secret {
    override def getUser: Option[String] = ??? // todo:
    override def getPwd: Option[String] = ???
  }

  abstract class ConnectionConfig
  abstract class JdbcConnectionConfig extends ConnectionConfig {
    def url: String
    val className: String // JDBC classname
    val table: String
    val quote: String = "`"
  }

  case class MySqlConfig(host: String, port: Int, db: String, table: String) extends JdbcConnectionConfig {
    override def url = s"jdbc:mysql://${host}:${port}/${db}"
    override val className: String =  "com.mysql.cj.jdbc.Driver"
  }

  case class RedisConfig(host: String, port: Int, db: Int=0) extends ConnectionConfig

  case class H2Config(path: String, dbname: String, table: String) extends JdbcConnectionConfig {
    override def url = s"jdbc:h2:${path}/${dbname}"
    override val className = "org.h2.Driver"
  }

  case class PostgreSqlConfig(host: String, port: Int, db: String, table: String) extends JdbcConnectionConfig {
    override def url = s"jdbc:postgresql://${host}:${port}/${db}"
    override val className: String =  "org.postgresql.Driver"
    override val quote: String = "\""
  }

  class SqliteConfig extends ConnectionConfig

  abstract class MongoConfig extends ConnectionConfig {
    def createConn: MongoClient
  }

  // uri: mongodb://hostname:port/
  case class MongoClientConfig(uri: String) extends MongoConfig {
    override def createConn: MongoClient = MongoClients.create(uri)

    override def toString: String = uri
  }

  def caseClassToMap(instance: AnyRef): Map[String, Any] = {
    instance.getClass.getDeclaredFields.map { f =>
      f.setAccessible(true)
      (f.getName -> f.get(instance))
    }.filterNot(_._1.contains('$')).toMap
  }
}

trait ReadFromDB[T] extends DataQuery[Iterable[T]]{
  def read(query: Map[String, Any]): Iterable[T]

  val config: DB.ConnectionConfig
  val secret: DB.Secret

  override def run(input: Map[String, Any]): Iterable[T] = {
    logger.info(s"${this.getClass.getName} reading from storage: $config")
    read(input)
 
  }
}

trait ReadIteratorFromDB[T] extends DataQuery[Iterator[T]]{
  def read(query: Map[String, Any]): Iterator[T]

  val config: DB.ConnectionConfig
  val secret: DB.Secret

  override def run(input: Map[String, Any]): Iterator[T] = {
    logger.info(s"${this.getClass.getName} reading iterator from storage: $config")
    read(input)
  }
}

trait WriteToDB[T] extends Workflow[Iterable[T], Iterable[T]]{
  def write(data: Iterable[T]): Iterable[T]

  val config: DB.ConnectionConfig
  val secret: DB.Secret

  override def run(input: Iterable[T]): Iterable[T] = {
    logger.info(s"${this.getClass.getName} writing to storage: $config")
    write(input)
    input
  }
}

trait Filter { 
  def toSql: String 
  def clean(quote: Char): Filter
}

case class AndFilter(cond: Filter*) extends Filter { 
  override def toSql: String = cond.map(_.toSql).mkString(" and ")
  override def clean(quote: Char): Filter = AndFilter(cond.map(_.clean(quote)):_*)
}

case class OrFilter(cond: Filter*) extends Filter { 
  override def toSql: String = cond.map(_.toSql).mkString(" or ")
  override def clean(quote: Char): Filter = OrFilter(cond.map(_.clean(quote)):_*)
}

case class Gt(field: String, value: Any) extends Filter { 
  override def toSql = s"$field > ${Filter.vts(value)}"
  override def clean(quote: Char): Filter = Gt(Filter.quote(Filter.esc(field), quote), value)
}

case class Gte(field: String, value: Any) extends Filter { 
  override def toSql = s"$field >= ${Filter.vts(value)}"
  override def clean(quote: Char): Filter = Gte(Filter.quote(Filter.esc(field), quote), value)
}

case class Lt(field: String, value: Any) extends Filter { 
  override def toSql = s"$field < ${Filter.vts(value)}"
  override def clean(quote: Char): Filter = Lt(Filter.quote(Filter.esc(field), quote), value)
}

case class Lte(field: String, value: Any) extends Filter { 
  override def toSql = s"$field <= ${Filter.vts(value)}"
  override def clean(quote: Char): Filter = Lte(Filter.quote(Filter.esc(field), quote), value)
}

case class Eq(field: String, value: Any) extends Filter { 
  override def toSql = s"$field == ${Filter.vts(value)}"
  override def clean(quote: Char): Filter = Eq(Filter.quote(Filter.esc(field), quote), value)
}

case class Not(cond: Filter) extends Filter { 
  override def toSql = s"not ${cond.toSql}"
  override def clean(quote: Char): Filter = Not(cond.clean(quote))
}

case class Between(field: String, lb: Any, ub: Any) extends Filter {
  override def toSql = s"($field between ${Filter.vts(lb)} and ${Filter.vts(ub)})"
  override def clean(quote: Char): Filter = Between(Filter.quote(Filter.esc(field), quote), lb, ub)
}
case class IsIn(field: String, values: Set[Any]) extends Filter {
  override def toSql: String = s"$field in (${values.map(Filter.vts).mkString(",")})"
  override def clean(quote: Char): Filter = IsIn(Filter.quote(Filter.esc(field), quote), values)
}

object Filter {
  def vts(value: Any): String = if (value.isInstanceOf[String])
    s"\"$value\""
  else value.toString

  def quote(f: String, quote: Char) = {
    val q = quote.toString
    val qf = if (!f.startsWith(q)) quote + f else f
    if (!qf.endsWith(q)) qf + quote else qf
  }

  def esc(f: String): String = f.replace("`", "\\`").replace("\"", "\\\"")
}
