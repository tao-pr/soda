package de.tao.soda.etl.data

import de.tao.soda.etl.{DataQuery, Workflow}
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.MongoClientSettings
import com.mongodb.connection.ClusterSettings
import com.mongodb.ServerAddress
import com.mongodb.MongoCredential
import com.mongodb.ConnectionString


object DB {
  trait Secret {
    def getUser: Option[String]
    def getPwd: Option[String]
    def asTuple = (getUser, getPwd)
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
    val quote: Char = '`'
  }

  case class MySqlConfig(host: String, port: Int, db: String, table: String) extends JdbcConnectionConfig {
    override def url = s"jdbc:mysql://${host}:${port}/${db}"
    override val className: String =  "com.mysql.cj.jdbc.Driver"
  }

  case class RedisConfig(host: String, port: Int, db: Int=0) extends ConnectionConfig

  abstract class H2Config extends JdbcConnectionConfig {
    override val className = "org.h2.Driver"
  }

  case class H2FileConfig(path: String, table: String, encrypt: Boolean, zip: Boolean=false) extends H2Config {
    private val sEncrypt = if (encrypt) ";CIPHER=AES" else ""
    private val sZip = if (zip) "zip:" else ""
    override def url = s"jdbc:h2:${sZip}${path}${sEncrypt}"
  }

  // Configure named db if we want to access DB from multiple threads or child processes on the same JVM
  // http://www.h2database.com/html/features.html#in_memory_databases
  case class H2MemConfig(namedDb: String, table: String) extends H2Config {
    override def url = s"jdbc:h2:mem:${namedDb}"
  }

  case class PostgreSqlConfig(host: String, port: Int, db: String, table: String) extends JdbcConnectionConfig {
    override def url = s"jdbc:postgresql://${host}:${port}/${db}"
    override val className: String =  "org.postgresql.Driver"
    override val quote: Char = '\"'
  }

  abstract class SqliteConfig extends JdbcConnectionConfig {
    override val className = "org.sqlite.JDBC"
    override val quote: Char = '\"'
  }

  case class SqliteFileConfig(path: String, table: String) extends SqliteConfig {
    override def url = s"jdbc:sqlite:${path}"
  }

  // Configure named db if we want to access DB from multiple threads or child processes on the same JVM
  // http://www.h2database.com/html/features.html#in_memory_databases
  case class SqliteMemConfig(namedDb: Option[String], table: String) extends SqliteConfig {
    override def url = s"jdbc:sqlite::memory:${namedDb.getOrElse("")}"
  }

  abstract class MongoConfig extends ConnectionConfig {
    val dbname: String
    def createConn(secret: DB.Secret): MongoClient
  }

  // connStr: mongodb://hostname:port/
  //          mongodb://hostname1:port/,hostname2:port/
  case class MongoClientConfig(connStr: String, override val dbname: String) extends MongoConfig {
    override def createConn(secret: DB.Secret): MongoClient = {
      val settingsBuild = MongoClientSettings.builder()
        .applyToClusterSettings((builder: ClusterSettings.Builder) => 
          builder.applyConnectionString(new ConnectionString(connStr)))

      val settingsWithCred = secret.asTuple match {
        case (None, _) => settingsBuild
        case (Some(usr), pwdOpt) => 
          val cred = MongoCredential.createCredential(
            usr, 
            dbname, 
            pwdOpt.map(_.toArray).getOrElse(Array.empty)
          )
          settingsBuild.credential(cred)
      }

      MongoClients.create(settingsWithCred.build())
    }

    override def toString: String = connStr
  }

  def caseClassToMap(instance: AnyRef): Map[String, Any] = {
    instance.getClass.getDeclaredFields.map { f =>
      f.setAccessible(true)
      (f.getName -> f.get(instance))
    }.filterNot(_._1.contains('$')).toMap
  }
}

trait DeleteFromDB extends Workflow[Filter, Unit]{
  def del(query: Filter): Unit
  
  val config: DB.ConnectionConfig
  val secret: DB.Secret

  override def run(input: Filter): Unit = {
    logger.info(s"${this.getClass().getName()} deleting from storage: $config")
    val n = del(input)
    logger.info(s"${this.getClass().getName()} deleted $n records")
  }
}

trait ReadFromDB[T] extends DataQuery[Iterable[T]]{
  def read(query: Filter): Iterable[T]

  val config: DB.ConnectionConfig
  val secret: DB.Secret

  override def run(input: Filter): Iterable[T] = {
    logger.info(s"${this.getClass.getName} reading from storage: $config")
    read(input)
  }
}

trait ReadIteratorFromDB[T] extends DataQuery[Iterator[T]]{
  def read(query: Filter): Iterator[T]

  val config: DB.ConnectionConfig
  val secret: DB.Secret

  override def run(input: Filter): Iterator[T] = {
    logger.info(s"${this.getClass.getName} reading iterator from storage: $config")
    read(input)
  }
}

/**
 * WriteToDB intends to "insert only" new data into the storage
 */
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

case object NFilter extends Filter {
  override def toSql: String = ""
  override def clean(quote: Char): Filter = NFilter
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
  override def toSql = s"$field = ${Filter.vts(value)}"
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

case class Like(field: String, value: String) extends Filter {
  override def toSql: String = s"$field like ${Filter.vts(value)}"
  override def clean(quote: Char): Filter = Like(Filter.quote(Filter.esc(field), quote), value)
}

case class IsNull(field: String) extends Filter {
  override def toSql: String = s"$field is NULL"
  override def clean(quote: Char): Filter = IsNull(Filter.quote(Filter.esc(field), quote))
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
