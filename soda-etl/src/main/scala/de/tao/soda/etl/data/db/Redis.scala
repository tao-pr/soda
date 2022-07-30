package de.tao.soda.etl.data.db

import com.redis.RedisClient
import com.redis.serialization.Parse
import de.tao.soda.etl.data.{DB, ReadFromDB, ReadIteratorFromDB, WriteToDB}
import de.tao.soda.etl.data.DB.RedisConfig

import scala.collection.mutable.ArrayBuffer

trait Redis {
  var conn: Option[RedisClient] = None

  def createConn(config: RedisConfig, secret: DB.Secret) = {
    if (conn.isEmpty)
      conn = Some(new RedisClient(config.host, config.port, config.db, secret = secret.getPwd))
  }
}

class ReadFromRedis[T](
override val config: RedisConfig,
override val secret: DB.Secret)
(implicit val parser: Parse[T]) extends ReadFromDB[(String,T)] with Redis {

  // For redis, [[query]] has to be a map of key -> field(s) to read from
  // eg.
  //      Map( "key1" -> "field1", "key2" -> ["field1","field2","field3"]
  //
  override def read(query: Map[String, Any]): Iterable[(String,T)] = {
    createConn(config, secret)
    conn.map{ cc =>
      query.flatten{ case (k,v) =>
        val fields = if (v.isInstanceOf[Iterable[_]])
          v.asInstanceOf[Iterable[String]].toSeq
        else
          v.toString :: Nil
        cc.hmget[String, T](k, fields:_*).getOrElse(Map.empty)
      }
    }.getOrElse(List.empty)
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromRedis: tearing down connection")
    conn.map(_.close())
  }
}

class WriteToRedis[T <: AnyRef](
override val config: RedisConfig,
override val secret: DB.Secret) extends WriteToDB[(String, String, T)] with Redis {

  // [[data]] has to be a tuple of (key -> field -> value)
  override def write(data: Iterable[(String, String, T)]): Iterable[(String, String, T)] = {
    createConn(config, secret)
    logger.info(s"WriteToRedis : Writing ${data.size} records")
    for { cc <- conn } yield {
      data.groupBy(_._1).foreach{ case (k, iter) =>
        cc.hmset(k, iter.map{ case(_,f,v) => (f,v)})
      }
    }
    data
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromRedis: tearing down connection")
    conn.map(_.close())
  }
}
