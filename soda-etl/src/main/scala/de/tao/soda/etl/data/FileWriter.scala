package de.tao.soda.etl.data

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import de.tao.soda.etl.DataWriter
import purecsv.unsafe._
import purecsv.unsafe.converter.RawFieldsConverter

import java.io._
import java.util.zip.GZIPOutputStream

case class CSVFileWriter[T <: Product with Serializable](filename: String, delimiter: Char)
  (implicit val rc: RawFieldsConverter[T])
  extends DataWriter[Iterable[T]] {

  override def run(input: Iterable[T]): String = {
    val headerOpt = input.headOption.map(_.productElementNames.toList)
    logger.info(s"CSVFileWriter writing file to $filename, with headers = $headerOpt")

    val csv = new CSVIterable[T](input)
    csv.writeCSVToFileName(filename, delimiter.toString, headerOpt)
    filename

  }
}

case class JSONFileWriter[T <: Product with Serializable](filename: String)(implicit clazz: Class[T]) extends DataWriter[T] {
  lazy val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: T): String = {
    logger.info(s"JSONFileWriter writing file to $filename")

    val ostream = new FileOutputStream(new File(filename))
    mapper.writeValue(ostream, input)
    filename
  }
}

case class TextFileWriter[T <: Product with Serializable](filename: String) extends DataWriter[Iterable[T]] {
  override def run(input: Iterable[T]): String = {
    logger.info(s"TextFileWriter writing file to $filename")
    val bufWriter = new BufferedWriter(new FileWriter(filename))
    input.foreach{ entry => bufWriter.write(entry.toString() + "\n") }
    bufWriter.close()
    filename
  }
}

case class ObjectWriter[T <: Product with Serializable](filename: String) extends DataWriter[T]{
  override def run(input: T): String = {
    logger.info(s"ObjectWriter writing to $filename")
    val writer = new ObjectOutputStream(new FileOutputStream(filename))
    writer.writeObject(input)
    writer.close()
    filename
  }
}

case class ObjectZippedWriter[T <: Product with Serializable](filename: String) extends DataWriter[T]{
  override def run(input: T): String = {
    logger.info(s"ObjectZippedWriter writing to $filename")
    val gzip = new GZIPOutputStream(new FileOutputStream(filename))
    val writer = new ObjectOutputStream(gzip)
    writer.writeObject(input)
    writer.close()
    filename
  }
}

object ObjectWriter {
  def saveToFile[T <: Product with Serializable](data: T, filename: String): String ={
    ObjectWriter(filename).run(data, false)
    filename
  }
}
