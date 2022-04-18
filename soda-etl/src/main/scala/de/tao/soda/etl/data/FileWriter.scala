package de.tao.soda.etl.data

import de.tao.soda.etl.DataWriter
import purecsv.unsafe.converter.RawFieldsConverter
import purecsv.unsafe._
import shapeless.HList

case class CSVFileWriter[T <: HList](filename: String, delimiter: Char)
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

case class TextFileWriter[T <: HList](filename: String) extends DataWriter[Iterable[T]] {
  override def run(input: Iterable[T], dry: Boolean): String = ???
}
