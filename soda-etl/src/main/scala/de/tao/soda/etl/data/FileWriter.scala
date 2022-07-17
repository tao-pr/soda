package de.tao.soda.etl.data

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import de.tao.soda.etl.{DataWriter, InputIdentifier, PathIdentifier, Workflow}
import purecsv.unsafe._
import purecsv.unsafe.converter.RawFieldsConverter

import java.io._
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.util.zip.GZIPOutputStream

trait OutputIdentifier
case class OutputPath(path: String) extends OutputIdentifier {
  override def toString: String = path
}
class RandomPath(prefix: String, ext: String, randomizer: () => (String)) extends OutputIdentifier {
  val _ext = if (ext.startsWith(".")) ext else "." + ext
  override def toString: String = s"${prefix}${randomizer()}${_ext}"
}
case class UUIDPath(prefix: String, ext: String) extends RandomPath(prefix, ext, ()=>java.util.UUID.randomUUID().toString)
case class TimestampPath(prefix: String, fmt: DateTimeFormatter, ext: String) extends RandomPath(prefix, ext, ()=>java.time.LocalDateTime.now().format(fmt))
case class TimestampUUIDPath(prefix: String, fmt: DateTimeFormatter, ext: String) extends RandomPath(prefix, ext, ()=>java.time.LocalDateTime.now().format(fmt) + "-" + java.util.UUID.randomUUID().toString)

object OutputIdentifier {
  def $(s: String) = OutputPath(s) // shorthand
}

case class EnsureDirExists[T](pathName: String) extends Workflow[T, T] {
  override def run(input: T): T = {
    logger.info(s"EnsureDirExists : ${pathName}")
    Files.createDirectories(Paths.get(pathName))
    input
  }
}

case class WriteAsCSV[T <: Product with Serializable](filename: OutputIdentifier, delimiter: Char)
  (implicit val rc: RawFieldsConverter[T])
  extends DataWriter[Iterable[T]] {

  override def run(input: Iterable[T]): InputIdentifier = {
    val headerOpt = input.headOption.map(_.productElementNames.toList)
    val fname = filename.toString
    logger.info(s"WriteAsCSV writing file to $fname, with headers = $headerOpt")

    val csv = new CSVIterable[T](input)
    csv.writeCSVToFileName(fname, delimiter.toString, headerOpt)
    PathIdentifier(fname)
  }
}

case class WriteIteratorAsCSV[T <: Product with Serializable](filename: OutputIdentifier, delimiter: Char)
  (implicit val rc: RawFieldsConverter[T])
  extends DataWriter[Iterator[T]] {

  override def run(input: Iterator[T]): InputIdentifier = {
    val list = input.toList
    val headerOpt = list.headOption.map(_.productElementNames.toList)
    val fname = filename.toString
    logger.info(s"WriteAsCSV writing file to $fname, with headers = $headerOpt")

    val csv = new CSVIterable[T](list)
    csv.writeCSVToFileName(fname, delimiter.toString, headerOpt)
    PathIdentifier(fname)
  }
}

case class WriteAsJSON[T <: Product with Serializable](filename: OutputIdentifier)(implicit clazz: Class[T]) extends DataWriter[T] {
  lazy val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: T): InputIdentifier = {
    val fname = filename.toString
    logger.info(s"WriteAsJSON writing file to $fname")

    val ostream = new FileOutputStream(new File(fname))
    mapper.writeValue(ostream, input)
    PathIdentifier(fname)
  }
}

case class WriteAsText[T <: Product with Serializable](filename: OutputIdentifier) extends DataWriter[Iterable[T]] {
  override def run(input: Iterable[T]): InputIdentifier = {
    val fname = filename.toString
    logger.info(s"WriteAsText writing file to $fname")
    val bufWriter = new BufferedWriter(new FileWriter(fname))
    input.foreach{ entry => bufWriter.write(entry.toString() + "\n") }
    bufWriter.close()
    PathIdentifier(fname)
  }
}

case class WriteAsObject[T <: Product with Serializable](filename: OutputIdentifier) extends DataWriter[T]{
  override def run(input: T): InputIdentifier = {
    val fname = filename.toString
    logger.info(s"WriteAsObject writing to $fname")
    val writer = new ObjectOutputStream(new FileOutputStream(fname))
    writer.writeObject(input)
    writer.close()
    PathIdentifier(fname)
  }
}

case class WriteAsZippedObject[T <: Product with Serializable](filename: OutputIdentifier) extends DataWriter[T]{
  override def run(input: T): InputIdentifier = {
    val fname = filename.toString
    logger.info(s"WriteAsZippedObject writing to $fname")
    val gzip = new GZIPOutputStream(new FileOutputStream(fname))
    val writer = new ObjectOutputStream(gzip)
    writer.writeObject(input)
    writer.close()
    PathIdentifier(fname)
  }
}

object WriteAsObject {
  def saveToFile[T <: Product with Serializable](data: T, filename: OutputIdentifier): InputIdentifier = {
    val fname = filename.toString
    WriteAsObject(filename).run(data, false)
  }
}
