package de.tao.soda.etl

import org.apache.spark.sql.DataFrame

import java.io.InputStream
import scala.io.BufferedSource

sealed trait InputIdentifier
sealed trait BufferedInputIdentifier

case class PathIdentifier(s: String, encoding: Option[String]=None) extends InputIdentifier {
  override def toString: String = s
}

case class SourceIdentifier(s: BufferedSource) extends InputIdentifier {
  override def toString: String = s.getClass.getName
}

case class StreamIdentifier(s: InputStream, encoding: String) extends InputIdentifier {
  override def toString: String = s.getClass.getName
}

private[etl] object ToSource {
  def apply(ii: InputIdentifier): BufferedSource = ii match {
    case PathIdentifier(s, None) => scala.io.Source.fromFile(s)
    case PathIdentifier(s, Some(e)) => scala.io.Source.fromFile(s, e)
    case SourceIdentifier(s) => s
    case StreamIdentifier(s, e) => scala.io.Source.fromInputStream(s, e)
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

case object InputToString extends Workflow[InputIdentifier, String]{
  override def run(input: InputIdentifier): String = input match {
    case PathIdentifier(s, _) => s
    case w => throw new UnsupportedOperationException(s"InputToString cannot parse ${w.getClass.getName} to string. Expect a PathIdentifier instead.")
  }
}


trait DataReader[T] extends Workflow[InputIdentifier, T]
trait DataLoader[T] extends Workflow[String, T]
trait DataQuery[T] extends Workflow[Map[String, Any], T]
trait DataDumper[T] extends Workflow[T, Unit]
trait DataWriter[T] extends Workflow[T, InputIdentifier]
trait DataIntercept[T] extends IsoWorkflow[T] {
  def intercept(data: T): Unit
  override def run(input: T): T = {
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

final class ToIterable[T] extends Workflow[Iterator[T], Iterable[T]]{
  override def run(input: Iterator[T]): Iterable[T] = {
    input.to(Iterable)
  }
}

final class ToIterator[T] extends Workflow[Iterable[T], Iterator[T]] {
  override def run(input: Iterable[T]): Iterator[T] = {
    input.iterator
  }
}

class LiftOption[T] extends Workflow[T, Option[T]]{
  override def run(input: T): Option[T] = Option(input)
}

class UnliftOption[T] extends Workflow[Option[T], T]{
  override def run(input: Option[T]): T = input.get
}

class LiftIter[T] extends Workflow[T, Iterable[T]]{
  override def run(input: T): Iterable[T] = Seq(input)
}


