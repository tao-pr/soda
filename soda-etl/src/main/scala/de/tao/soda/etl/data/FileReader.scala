package de.tao.soda.etl.data

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import de.tao.soda.etl._
import purecsv.unsafe._
import purecsv.unsafe.converter.RawFieldsConverter

import java.io.{ByteArrayInputStream, File, FileInputStream, ObjectInputStream}
import java.util.zip.GZIPInputStream
import scala.reflect.ClassTag


class ReadAsStream[T](encoding: String) extends DataLoader[InputIdentifier]{
  override def run(input: String): InputIdentifier = {
    logger.info(s"StreamReader is reading from $input")
    val stream = new FileInputStream(input)
    StreamIdentifier(stream, encoding)
  }
}

object ReadAsTextLines extends DataReader[Iterable[String]]{
  override def run(input: InputIdentifier): Iterable[String] = {
    logger.info(s"ReadAsTextLines reading from ${input}")
    val src = ToSource(input)
    val lines = src.getLines().toSeq
    src.close()
    lines
  }
}

object ReadAsTextLinesIterator extends DataReader[Iterator[String]]{
  override def run(input: InputIdentifier) = {
    logger.info(s"ReadAsTextLinesIterator reading from $input")
    val src = ToSource(input)
    val lines = src.getLines()
    src.close()
    lines
  }
}

case class ReadCSV[T <: Product with Serializable](delimiter: Char)
(implicit val classTag: ClassTag[T], val converter: RawFieldsConverter[T]) extends DataReader[Iterator[T]]{
  override def run(input: InputIdentifier) = {
    logger.info(s"ReadCSV reading ${classTag} from $input with delimiter=$delimiter")
    val streamReader = ToSource(input).reader()
    val iter = CSVReader[T].readCSVFromReader(streamReader, delimiter)
    //streamReader.close()
    iter
  }
}

case class ReadCSVFromString[T <: Product with Serializable](delimiter: Char)
(implicit val classTag: ClassTag[T], val converter: RawFieldsConverter[T]) extends Workflow[String, Iterator[T]]{
  override def run(input: String): Iterator[T] = {
    logger.info(s"CSVIterReader reading ${classTag} from an iterable")
    CSVReader[T].readCSVFromString(input, delimiter).iterator
  }
}

class ReadJSON[T <: Product with Serializable](implicit clazz: Class[T]) extends DataReader[T]{
  lazy val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: InputIdentifier) = {
    logger.info(s"ReadJSON reading ${clazz.getName} from $input")
    input match {
      case PathIdentifier(s, encoding) =>
        mapper.readValue[T](new java.io.File(s), clazz)
      case SourceIdentifier(s) =>
        val content = s.getLines().mkString(" ")
        mapper.readValue[T](content, clazz)
      case StreamIdentifier(s, encoding) =>
        mapper.readValue[T](s, clazz)
    }
  }
}

class ReadAsObjectOpt[T <: Product with Serializable] extends DataReader[Option[T]]{
  override def run(input: InputIdentifier): Option[T] = {
    logger.info(s"ReadAsObjectOpt reading from $input")
    input match {
      case PathIdentifier(s, _) =>
        val filesize = new File(s).length()
        logger.info(s"ReadAsObjectOpt reading ${filesize} bytes from file")
        val reader = new ObjectInputStream(new FileInputStream(s))
        val data = Option(reader.readObject().asInstanceOf[T])
        reader.close()
        data

      case SourceIdentifier(s) =>
        val sreader = s.reader()
        val bytes = LazyList.continually(sreader.read).takeWhile(_ != -1).map(_.toByte).toArray
        logger.info(s"ReadAsObjectOpt reading ${bytes.length} bytes from file")
        val reader = new ObjectInputStream(new ByteArrayInputStream(bytes))
        val data = Option(reader.readObject().asInstanceOf[T])
        sreader.close()
        reader.close()
        data

      case StreamIdentifier(s, _) =>
        logger.info(s"ReadAsObjectOpt reading from stream")
        val reader = new ObjectInputStream(s)
        val data = Option(reader.readObject().asInstanceOf[T])
        reader.close()
        data
    }
  }
}

class ReadZippedAsObject[T <: Product with Serializable] extends DataReader[T]{
  override def run(input: InputIdentifier): T = {
    logger.info(s"ReadZippedAsObject reading from $input")
    input match {
      case PathIdentifier(s, _) =>
        val filesize = new File(s).length()
        logger.info(s"ReadZippedAsObject reading ${filesize} bytes from file")
        val gzip = new GZIPInputStream(new FileInputStream(s))
        val reader = new ObjectInputStream(gzip)
        val data = reader.readObject().asInstanceOf[T]
        reader.close()
        data

      case SourceIdentifier(s) =>
        val sreader = s.reader()
        val bytes = LazyList.continually(sreader.read).takeWhile(_ != -1).map(_.toByte).toArray
        logger.info(s"ReadZippedAsObject reading ${bytes.length} bytes from file")
        val gzip = new GZIPInputStream(new ByteArrayInputStream(bytes))
        val reader = new ObjectInputStream(gzip)
        val data = reader.readObject().asInstanceOf[T]
        sreader.close()
        reader.close()
        data

      case StreamIdentifier(s, _) =>
        logger.info(s"ReadZippedAsObject reading from stream")
        val gzip = new GZIPInputStream(s)
        val reader = new ObjectInputStream(gzip)
        val data = reader.readObject().asInstanceOf[T]
        reader.close()
        data
    }
  }
}

final object ReadAsObjectOpt {
  def loadFromFile[T <: Product with Serializable](filename: String): Option[T] = {
    val input = PathIdentifier(filename)
    new ReadAsObjectOpt[T].run(input)
  }
}