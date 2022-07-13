package de.tao.soda.etl.data

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import de.tao.soda.etl.{DataWriter, InputIdentifier, PathIdentifier}
import purecsv.unsafe._
import purecsv.unsafe.converter.RawFieldsConverter

import java.io._
import java.util.zip.GZIPOutputStream

case class CSVFileWriter[T <: Product with Serializable](filename: String, delimiter: Char)
  (implicit val rc: RawFieldsConverter[T])
  extends DataWriter[Iterable[T]] {

  override def run(input: Iterable[T]): InputIdentifier = {
    val headerOpt = input.headOption.map(_.productElementNames.toList)
    logger.info(s"CSVFileWriter writing file to $filename, with headers = $headerOpt")

    val csv = new CSVIterable[T](input)
    csv.writeCSVToFileName(filename, delimiter.toString, headerOpt)
    PathIdentifier(filename)
  }
}

case class CSVFileIteratorWriter[T <: Product with Serializable](filename: String, delimiter: Char)
(implicit val rc: RawFieldsConverter[T])
  extends DataWriter[Iterator[T]] {

  override def run(input: Iterator[T]): InputIdentifier = {
    val list = input.toList
    val headerOpt = list.headOption.map(_.productElementNames.toList)
    logger.info(s"CSVFileWriter writing file to $filename, with headers = $headerOpt")

    val csv = new CSVIterable[T](list)
    csv.writeCSVToFileName(filename, delimiter.toString, headerOpt)
    PathIdentifier(filename)
  }
}

case class JSONFileWriter[T <: Product with Serializable](filename: String)(implicit clazz: Class[T]) extends DataWriter[T] {
  lazy val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: T): InputIdentifier = {
    logger.info(s"JSONFileWriter writing file to $filename")

    val ostream = new FileOutputStream(new File(filename))
    mapper.writeValue(ostream, input)
    PathIdentifier(filename)
  }
}

case class TextFileWriter[T <: Product with Serializable](filename: String) extends DataWriter[Iterable[T]] {
  override def run(input: Iterable[T]): InputIdentifier = {
    logger.info(s"TextFileWriter writing file to $filename")
    val bufWriter = new BufferedWriter(new FileWriter(filename))
    input.foreach{ entry => bufWriter.write(entry.toString() + "\n") }
    bufWriter.close()
    PathIdentifier(filename)
  }
}

case class ObjectWriter[T <: Product with Serializable](filename: String) extends DataWriter[T]{
  override def run(input: T): InputIdentifier = {
    logger.info(s"ObjectWriter writing to $filename")
    val writer = new ObjectOutputStream(new FileOutputStream(filename))
    writer.writeObject(input)
    writer.close()
    PathIdentifier(filename)
  }
}

case class ObjectZippedWriter[T <: Product with Serializable](filename: String) extends DataWriter[T]{
  override def run(input: T): InputIdentifier = {
    logger.info(s"ObjectZippedWriter writing to $filename")
    val gzip = new GZIPOutputStream(new FileOutputStream(filename))
    val writer = new ObjectOutputStream(gzip)
    writer.writeObject(input)
    writer.close()
    PathIdentifier(filename)
  }
}

object ObjectWriter {
  def saveToFile[T <: Product with Serializable](data: T, filename: String): InputIdentifier ={
    ObjectWriter(filename).run(data, false)
    PathIdentifier(filename)
  }
}
