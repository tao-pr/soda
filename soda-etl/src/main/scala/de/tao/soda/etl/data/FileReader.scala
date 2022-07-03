package de.tao.soda.etl.data

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import de.tao.soda.etl._
import purecsv.unsafe._
import purecsv.unsafe.converter.RawFieldsConverter

import java.io.{ByteArrayInputStream, File, FileInputStream, ObjectInputStream}
import java.util.zip.GZIPInputStream
import scala.reflect.ClassTag


object TextFileReader extends DataReader[Iterable[String]]{
  override def run(input: InputIdentifier, dry: Boolean): Iterable[String] = {
    if (dry) {
      logger.info(s"TextFileReader to read from ${input}")
      Iterable.empty
    } else {
      logger.info(s"TextFileReader reading from ${input}")
      val src = ToSource(input)
      val lines = src.getLines().toSeq
      src.close()
      lines
    }
  }
}

object TextFileBufferedReader extends DataReader[Iterator[String]]{
  override def run(input: InputIdentifier, dry: Boolean) = {
    if (dry) {
      logger.info(s"TextFileBufferedReader to read from $input")
      Iterator.empty
    } else {
      logger.info(s"TextFileBufferedReader reading from $input")
      val src = ToSource(input)
      val lines = src.getLines()
      src.close()
      lines
    }
  }
}

case class CSVFileReader[T <: Product with Serializable](delimiter: Char)
(implicit val classTag: ClassTag[T], val converter: RawFieldsConverter[T]) extends DataReader[Iterator[T]]{
  override def run(input: InputIdentifier, dry: Boolean) = {
    if (dry){
      logger.info(s"CSVFileReader to read ${classTag} from $input with delimiter=$delimiter")
      Iterator.empty[T]
    }
    else {
      logger.info(s"CSVFileReader reading ${classTag} from $input with delimiter=$delimiter")
      val streamReader = ToSource(input).reader()
      val iter = CSVReader[T].readCSVFromReader(streamReader, delimiter)
      //streamReader.close()
      iter
    }
  }
}

class JSONReader[T <: Product with Serializable](implicit clazz: Class[T]) extends DataReader[T]{
  lazy val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: InputIdentifier, dry: Boolean) = {
    logger.info(s"JSONReader reading ${clazz.getName} from $input")
    input match {
      case PathIdentifier(s, encoding) =>
        mapper.readValue[T](new java.io.File(s), clazz)
      case SourceIdentifier(s) =>
        val content = s.getLines().mkString(" ")
        mapper.readValue[T](content, clazz)
    }
  }
}

class ObjectReader[T <: Product with Serializable] extends DataReader[Option[T]]{
  override def run(input: InputIdentifier, dry: Boolean): Option[T] = {
    if (dry){
      logger.info(s"ObjectReader to read from $input")
      None
    }
    else {
      logger.info(s"ObjectReader reading from $input")
      input match {
        case PathIdentifier(s, _) =>
          val filesize = new File(s).length()
          logger.info(s"ObjectReader reading ${filesize} bytes from file")
          val reader = new ObjectInputStream(new FileInputStream(s))
          val data = Option(reader.readObject().asInstanceOf[T])
          reader.close()
          data

        case SourceIdentifier(s) =>
          val sreader = s.reader()
          val bytes = LazyList.continually(sreader.read).takeWhile(_ != -1).map(_.toByte).toArray
          logger.info(s"ObjectReader reading ${bytes.length} bytes from file")
          val reader = new ObjectInputStream(new ByteArrayInputStream(bytes))
          val data = Option(reader.readObject().asInstanceOf[T])
          sreader.close()
          reader.close()
          data
      }
    }
  }
}

class ObjectZippedReader[T <: Product with Serializable] extends DataReader[T]{
  override def run(input: InputIdentifier, dry: Boolean): T = {
    logger.info(s"ObjectZippedReader reading from $input")
    input match {
      case PathIdentifier(s, _) =>
        val filesize = new File(s).length()
        logger.info(s"ObjectZippedReader reading ${filesize} bytes from file")
        val gzip = new GZIPInputStream(new FileInputStream(s))
        val reader = new ObjectInputStream(gzip)
        val data = reader.readObject().asInstanceOf[T]
        reader.close()
        data

      case SourceIdentifier(s) =>
        val sreader = s.reader()
        val bytes = LazyList.continually(sreader.read).takeWhile(_ != -1).map(_.toByte).toArray
        logger.info(s"ObjectZippedReader reading ${bytes.length} bytes from file")
        val gzip = new GZIPInputStream(new ByteArrayInputStream(bytes))
        val reader = new ObjectInputStream(gzip)
        val data = reader.readObject().asInstanceOf[T]
        sreader.close()
        reader.close()
        data
    }
  }
}

final object ObjectReader {
  def loadFromFile[T <: Product with Serializable](filename: String): Option[T] = {
    val input = PathIdentifier(filename)
    new ObjectReader[T].run(input)
  }
}