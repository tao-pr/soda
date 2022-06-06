package de.tao.soda.etl

import org.apache.spark.sql.DataFrame

import scala.io.BufferedSource

sealed trait InputIdentifier
sealed trait BufferedInputIdentifier

case class PathIdentifier(s: String, encoding: Option[String]=None) extends InputIdentifier
case class SourceIdentifier(s: BufferedSource) extends InputIdentifier

private[etl] object ToSource {
  def apply(ii: InputIdentifier): BufferedSource = ii match {
    case PathIdentifier(s, None) => scala.io.Source.fromFile(s)
    case PathIdentifier(s, Some(e)) => scala.io.Source.fromFile(s, e)
    case SourceIdentifier(s) => s
  }
}

object Implicits {
  implicit class StringImpl(val s: String) extends AnyVal {
    def lift = PathIdentifier(s)
  }
  implicit class SourceImpl(val s: BufferedSource) extends AnyVal {
    def lift = SourceIdentifier(s)
  }
}


trait DataReader[T] extends Workflow[InputIdentifier, T]
trait DataLoader[T] extends Workflow[String, T]
trait DataDumper[T] extends Workflow[T, String]
trait DataWriter[T] extends Workflow[T, String]
trait DataIntercept[T] extends IsoWorkflow[T] {
  def intercept(data: T): Unit
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

