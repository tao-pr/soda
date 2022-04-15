package de.tao.soda.etl.data

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import de.tao.soda.etl.DataReader
import purecsv.unsafe._

import scala.reflect.ClassTag

abstract class IterReader[T] extends DataReader[Iterable[T]]

class TextFileReader extends IterReader[String]{
  override def run(input: String, dry: Boolean) = {
    scala.io.Source.fromFile(input).getLines().toSeq
  }
}

case class CSVFileReader[T](delimiter: Char)(implicit val classTag: ClassTag[T]) extends IterReader[T]{
  override def run(input: String, dry: Boolean): List[T] = {
    CSVReader[T].readCSVFromFileName(input, delimiter)
  }
}

class JSONReader[T] extends DataReader[T]{
  val mapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: String, dry: Boolean) = {
    val file = new java.io.File(input)
    mapper.readValue[T](file, Class[T])
  }
}