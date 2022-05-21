package de.tao.soda.etl.data

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import de.tao.soda.etl.{DataReader, InputIdentifier, PathIdentifier, ToSource}
import purecsv.unsafe.converter.RawFieldsConverter
import purecsv.unsafe._

import java.io.InputStream
import scala.io.{BufferedSource, Source}
import scala.reflect.ClassTag


object TextFileReader extends DataReader[Iterable[String]]{
  override def run(input: InputIdentifier, dry: Boolean) = {
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
      logger.info(s"CSVFileReader to read ${classTag.getClass.getName} from $input with delimiter=$delimiter")
      Iterator.empty[T]
    }
    else {
      logger.info(s"CSVFileReader reading ${classTag.getClass.getName} from $input with delimiter=$delimiter")
      val streamReader = ToSource(input).reader()
      val iter = CSVReader[T].readCSVFromReader(streamReader, delimiter)
      //streamReader.close()
      iter
    }
  }
}

class JSONReader[T <: Product with Serializable](implicit clazz: Class[T]) extends DataReader[Option[T]]{
  val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: InputIdentifier, dry: Boolean) = {
    if (dry){
      logger.info(s"JSONReader to read ${clazz.getName} from $input")
      None
    }
    else {
      logger.info(s"JSONReader reading $clazz.getName} from $input")
      val file = input match {
        case PathIdentifier(s, encoding) => new java.io.File(s)
        case a => throw new UnsupportedOperationException(s"JSONReader expects path string input, got : ${a.getClass.getName} instead")
      }
      val parsed: T = mapper.readValue[T](file, clazz)
      Some(parsed)
    }
  }
}