package de.tao.soda.etl.data

import de.tao.soda.etl.DataWriter
import purecsv.unsafe._

abstract class FileWriter[T] extends DataWriter[T]

case class CSVFileWriter[T](filename: String, delimiter: Char) extends FileWriter[Iterable[T]] {
  override def run(input: Iterable[T], dry: Boolean): String = {
    input.writeCSVToFileName(filename, delimiter.toString)
    filename
  }
}

case class TextFileWriter[T](filename: String) extends FileWriter[Iterable[T]] {
  override def run(input: Iterable[T], dry: Boolean): String = ???
}
