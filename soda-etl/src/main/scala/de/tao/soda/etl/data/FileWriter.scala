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

  override def run(input: Iterable[T], dry: Boolean): String = {
    val headerOpt = input.headOption.map(_.productElementNames.toList)
    if (dry){
      logger.info(s"CSVFileWriter to write to $filename, with headers = $headerOpt")
      filename
    }
    else {
      logger.info(s"CSVFileWriter writing file to $filename, with headers = $headerOpt")

      val csv = new CSVIterable[T](input)
      csv.writeCSVToFileName(filename, delimiter.toString, headerOpt)
      filename
    }
  }
}

case class JSONFileWriter[T <: Product with Serializable](filename: String)(implicit clazz: Class[T]) extends DataWriter[Option[T]] {
  lazy val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: Option[T], dry: Boolean): String = {
    if (input.isEmpty){
      logger.info(s"JSONFileWriter will not write anything because input is empty.")
      filename
    }
    else if (dry){
      logger.info(s"JSONFileWriter to write to $filename")
      filename
    }
    else {
      logger.info(s"JSONFileWriter writing file to $filename")

      val ostream = new FileOutputStream(new File(filename))
      mapper.writeValue(ostream, input)
      filename
    }
  }
}

case class TextFileWriter[T <: Product with Serializable](filename: String) extends DataWriter[Iterable[T]] {
  override def run(input: Iterable[T], dry: Boolean): String = {
    if (dry){
      logger.info(s"TextFileWriter to write to $filename")
      filename
    }
    else {
      logger.info(s"TextFileWriter writing file to $filename")
      val bufWriter = new BufferedWriter(new FileWriter(filename))
      input.foreach{ entry => bufWriter.write(entry.toString() + "\n") }
      bufWriter.close()
      filename
    }
  }
}

case class ObjectWriter[T <: Product with Serializable](filename: String) extends DataWriter[T]{
  override def run(input: T, dry: Boolean): String = {
    if (dry){
      logger.info(s"ObjectWriter to write to $filename")
      filename
    }
    else{
      logger.info(s"ObjectWriter writing to $filename")
      val writer = new ObjectOutputStream(new FileOutputStream(filename))
      writer.writeObject(input)
      writer.close()
      filename
    }
  }
}

case class ObjectZippedWriter[T <: Product with Serializable](filename: String) extends DataWriter[T]{
  override def run(input: T, dry: Boolean): String = {
    if (dry){
      logger.info(s"ObjectZippedWriter to write to $filename")
      filename
    }
    else{
      logger.info(s"ObjectZippedWriter writing to $filename")
      val gzip = new GZIPOutputStream(new FileOutputStream(filename))
      val writer = new ObjectOutputStream(gzip)
      writer.writeObject(input)
      writer.close()
      filename
    }
  }
}

object ObjectWriter {
  def saveToFile[T <: Product with Serializable](data: T, filename: String): String ={
    ObjectWriter(filename).run(data, false)
    filename
  }
}
