package de.tao.soda.etl.data.db

import scala.reflect.ClassTag
import scala.collection.mutable
import com.mongodb.client.MongoCollection
import org.bson.Document
import com.mongodb.client.MongoClient
import com.mongodb.client.model.Filters._
import com.mongodb.client.model.Filters.{eq => eql}
import org.bson.conversions.Bson

import de.tao.soda.etl.data.{Filter, NFilter}
import de.tao.soda.etl.data.{DB, ReadFromDB, ReadIteratorFromDB, WriteToDB}
import de.tao.soda.etl.data._
import com.mongodb.client.FindIterable

trait MongoBase {
  var conn: Option[MongoClient] = None

  def createConn(config: DB.MongoConfig, secret: DB.Secret) = {
    if (conn.isEmpty)
      conn = Some(config.createConn)
    conn
  }

  def collection(dbname: String, collname: String): Option[MongoCollection[Document]] = 
    conn.map(_.getDatabase(dbname).getCollection(collname))

  def bsonFilter(filter: Filter): Bson = filter match {
    case AndFilter(t @_*) => and(t.map(bsonFilter(_)):_*)
    case OrFilter(t @_*) => or(t.map(bsonFilter(_)):_*)
    case Not(t) => not(bsonFilter(t))
    case Eq(t, v) => eql(t, v)
    case Gt(t, v) => gt(t, v)
    case Gte(t, v) => gte(t, v)
    case Lt(t, v) => lt(t, v)
    case Lte(t, v) => lte(t, v)
    case Between(t, v1, v2) => and(gte(t, v1), lte(t, v2))
    case IsIn(t, vs) => in(t, vs)
    case Like(t, v) => eql(t, v)
  }

}

class ReadFromMongo[T <: Product with Serializable](
  override val config: DB.MongoConfig,
  override val secret: DB.Secret,
  db: String,
  cl: String,
  limit: Option[Int]=None
)(implicit ct: ClassTag[T], cv: Document => T) extends ReadFromDB[T] with MongoBase {

  override def read(query: Filter): Iterable[T] = {
    createConn(config, secret)
    collection(db, cl) match {
      case None => 
        logger.warn("ReadFromMongo: cannot read from $config")
        Iterable.empty

      case Some(col) =>
        logger.info(s"ReadFromMongo: reading from $config")
        val find: FindIterable[Document] = query match {
          case NFilter => col.find()
          case _ => col.find(bsonFilter(query))
        }

        val lfind: FindIterable[Document] = limit.map(find.limit(_)).getOrElse(find)
        val buff = new mutable.ArrayBuffer[T]()
        lfind.forEach{ doc => 
          buff.addOne(cv(doc))
        }
        buff.toSeq
    }
  }

  override def shutdownHook(): Unit = {
    logger.info("ReadFromMongo: tearing down connection")
    conn.map(_.close())
  }
}