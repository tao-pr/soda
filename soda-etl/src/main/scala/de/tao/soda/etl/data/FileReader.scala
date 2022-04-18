package de.tao.soda.etl.data

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import de.tao.soda.etl.DataReader
import purecsv.unsafe.converter.RawFieldsConverter
import purecsv.unsafe._
import shapeless.HList

import scala.reflect.ClassTag


class TextFileReader extends DataReader[Iterable[String]]{
  override def run(input: String, dry: Boolean) = {
    if (dry) {
      logger.info(s"TextFileReader to read from ${input}")
      Iterable.empty
    } else {
      logger.info(s"TextFileReader reading from ${input}")
      val src = scala.io.Source.fromFile(input)
      val lines = src.getLines().toSeq
      src.close()
      lines
    }
  }
}

class TextFileBufferedReader(encoding: Option[String]=None) extends DataReader[Iterator[String]]{
  override def run(input: String, dry: Boolean) = {
    if (dry) {
      logger.info(s"TextFileBufferedReader to read from $input, encoding = ${encoding}")
      Iterator.empty
    } else {
      logger.info(s"TextFileBufferedReader reading from $input, encoding = ${encoding}")
      val src = encoding match {
        case Some(e) =>  scala.io.Source.fromFile(input, e)
        case _ =>  scala.io.Source.fromFile(input)
      }
      val lines = src.getLines()
      src.close()
      lines
    }
  }
}

// TODO: need to find better way to pass converter in
case class CSVFileReader[T <: HList](delimiter: Char, encoding: Option[String]=None)
(implicit val classTag: ClassTag[T], val converter: RawFieldsConverter[T]) extends DataReader[Iterator[T]]{
  override def run(input: String, dry: Boolean) = {
    if (dry){
      logger.info(s"CSVFileReader to read ${classTag.getClass.getName} from $input with delimiter=$delimiter")
      Iterator.empty[T]
    }
    else {
      logger.info(s"CSVFileReader reading ${classTag.getClass.getName} from $input with delimiter=$delimiter")
      val streamReader = encoding match {
        case Some(e) => scala.io.Source.fromFile(input, e).reader()
        case _ => scala.io.Source.fromFile(input).reader()
      }
      val iter = CSVReader[T].readCSVFromReader(streamReader, delimiter)
      streamReader.close()
      iter
    }
  }
}

class JSONReader[T <: HList](implicit clazz: Class[T]) extends DataReader[Option[T]]{
  val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: String, dry: Boolean) = {
    if (dry){
      logger.info(s"JSONReader to read ${clazz.getName} from $input")
      None
    }
    else {
      logger.info(s"JSONReader reading $clazz.getName} from $input")
      val file = new java.io.File(input)
      val parsed: T = mapper.readValue[T](file, clazz)
      Some(parsed)
    }
  }
}