package de.tao.soda.etl

import org.apache.spark.sql.DataFrame

import java.io.InputStream

trait DataReader[T] extends Workflow[String, T]
trait DataWriter[T] extends Workflow[T, String]
trait DataIntercept[T] extends Workflow[T, T] {
  def intercept(data: T)
  override def run(input: T, dry: Boolean): T = {
    intercept(input)
    input
  }
}

abstract class DataPeek[T](title: String, numRecords: Option[Int]=None, isOn: Boolean=true) extends DataIntercept[T] {
  protected def peek(data: T): Unit
  override def intercept(data: T): Unit = {

    if (isOn) {
      logger.info(s"DataPeek : ${title} (up to ${numRecords} records)")
      peek(data)
    }
  }
}

case class DataFramePeek(title: String, numRecords: Option[Int]=None, isOn: Boolean=true)
extends DataPeek[DataFrame](title, numRecords, isOn) {
  override protected def peek(data: DataFrame): Unit = {
    data.show(numRecords.getOrElse(20), false)
  }
}

case class StreamPeek(title: String, numRecords: Option[Int]=None, isOn: Boolean=true)
extends DataPeek[InputStream](title, numRecords, isOn){
  override protected def peek(data: InputStream): Unit = {
    // TODO: clone stream to bytestream array
    ???
  }
}
