package de.tao.soda.etl.data.db

import com.redis.RedisClient
import com.redis.serialization.Parse
import de.tao.soda.etl.Workflow
import de.tao.soda.etl.data.DB.RedisConfig
import de.tao.soda.etl.data.{DB, ReadFromDB, WriteToDB}

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
(implicit val parser: Parse[T]) extends ReadFromDB[(String, String, T)] with Redis {

  // For redis, [[query]] has to be a map of key -> field(s) to read from
  // eg.
  //      Map( "key1" -> "field1", "key2" -> ["field1","field2","field3"]
  //
  override def read(query: Map[String, Any]): Iterable[(String, String, T)] = {
    createConn(config, secret)
    conn.map{ cc =>
      query.flatten{ case (k,v) =>
        val fields = if (v.isInstanceOf[Iterable[_]])
          v.asInstanceOf[Iterable[String]].toSeq
        else
          v.toString :: Nil
        cc.hmget[String, T](k, fields:_*).getOrElse(Map.empty).map{ case (f, _v) => (k, f, _v)}
      }
    }.getOrElse(List.empty)
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
override val secret: DB.Secret) extends WriteToDB[(String, String, T)] with Redis {

  // [[data]] has to be a tuple of (key -> field -> value)
  override def write(data: Iterable[(String, String, T)]): Iterable[(String, String, T)] = {
    createConn(config, secret)
    logger.info(s"WriteToRedis : Writing ${data.size} records")
    for { cc <- conn } yield {
      data.groupBy(_._1).foreach{ case (k, iter) =>
        cc.hmset(k, iter.map{ case(_,f,v) => (f,v)}.toMap)
      }
    }
    data
  }

  override def shutdownHook(): Unit = {
    logger.info(s"ReadFromRedis: tearing down connection")
    conn.map(_.close())
  }
}
