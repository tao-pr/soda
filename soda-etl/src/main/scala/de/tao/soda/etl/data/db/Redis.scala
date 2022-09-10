package de.tao.soda.etl.data.db

import com.redis.RedisClient
import com.redis.serialization.Parse
import de.tao.soda.etl.Workflow
import de.tao.soda.etl.data.DB.RedisConfig
import de.tao.soda.etl.data.{DB, ReadFromDB, WriteToDB, Filter}
import de.tao.soda.etl.data.NFilter
import de.tao.soda.etl.data.AndFilter
import scala.collection.immutable

trait Redis {
  var conn: Option[RedisClient] = None

  def createConn(config: RedisConfig, secret: DB.Secret) = {
    if (conn.isEmpty)
      conn = Some(new RedisClient(config.host, config.port, config.db, secret = secret.getPwd))
  }
}

object Redis {
  type Key = Either[String, (String, String)]
}

trait RedisFilter extends Filter {
  override def toSql: String = ???
  override def clean(quote: Char): Filter = ???
}

case class MGet(keys: String*) extends RedisFilter
case class HMGet(key: String, fields: String*) extends RedisFilter
case class MultiHMGet(gets: HMGet*) extends RedisFilter

class ReadFromRedis[T](
override val config: RedisConfig,
override val secret: DB.Secret)
(implicit val parser: Parse[T]) extends ReadFromDB[(Redis.Key, Option[T])] with Redis {

  // For redis, [[query]] has to be a map of key -> field(s) to read from
  // eg.
  //      Map( "key1" -> "field1", "key2" -> ["field1","field2","field3"]
  //
  // The output is an iterable of a tuple containing:
  // - hashmap key (optional)
  // - field 
  // - value (None if not found in redis)
  override def read(query: Filter): Iterable[(Redis.Key, Option[T])] = {
    createConn(config, secret)
    conn.map{ cc =>
      def hmget(key: String, fields: String*): Iterable[(Redis.Key, Option[T])] = {
        val map: Map[String,T] = cc
        .hmget[String, T](key, fields:_*)
        .getOrElse(Map.empty)

        fields.map{ f => (Right((key, f)), map.get(f)) }
      }

      val res: Iterable[(Redis.Key, Option[T])] = query match {
        case MGet(keys @_*) => cc
          .mget[T](keys.head, keys.tail:_*)
          .map{ vs => keys.zip(vs).map{ case (k,v) => (Left(k), v) } }
          .getOrElse(Nil)

        case HMGet(key, fields @_*) => hmget(key, fields:_*)

        case MultiHMGet(hs @_*) => hs.flatten{ h =>
          hmget(h.key, h.fields:_*)
        }

        case _ => throw new UnsupportedClassVersionError("ReadFromRedis expects query of type RedisFilter")
      }
      res
    }.getOrElse(Nil)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromRedis: tearing down connection")
    conn.map(_.close())
  }
}

class FlushRedis[T](config: RedisConfig, secret: DB.Secret) extends Workflow[T,T] with Redis {
  override def run(input: T): T = {
    logger.info("FlushRedis : flushing db $config")
    for (cc <- conn){
      cc.flushdb
    }
    input
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromRedis: tearing down connection")
    conn.map(_.close())
  }
}

class WriteToRedis[T <: AnyRef](
override val config: RedisConfig,
override val secret: DB.Secret,
expireF: (String => Option[Int]) = (_ => None)) extends WriteToDB[(String, String, T)] with Redis {

  // [[data]] has to be a tuple of (key -> field -> value)
  override def write(data: Iterable[(String, String, T)]): Iterable[(String, String, T)] = {
    createConn(config, secret)
    logger.info(s"WriteToRedis : Writing ${data.size} records")
    for { cc <- conn } yield {
      data.groupBy(_._1).foreach{ case (k, iter) =>
        cc.hmset(k, iter.map{ case(_,f,v) => (f,v)}.toMap)
        expireF(k).foreach{ cc.expire(k,_) }
      }
    }
    data
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromRedis: tearing down connection")
    conn.map(_.close())
  }
}
