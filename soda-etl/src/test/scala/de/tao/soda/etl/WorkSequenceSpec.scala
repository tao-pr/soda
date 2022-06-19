package de.tao.soda.etl

import de.tao.soda.etl.data.{CSVFileReader, CSVFileWriter, JSONFileWriter}
import de.tao.soda.etl.workflow.{MapIter, WorkSequence}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import Domain._
import org.apache.commons.lang.NotImplementedException
import purecsv.unsafe.converter.RawFieldsConverter

class WorkSequenceSpec extends AnyFlatSpec with BeforeAndAfter {

  it should "connect workflows together" in {
    val wfread: DataReader[Iterator[CSVData]] = CSVFileReader[CSVData](',')
    val trans = new IteratorToIterable[CSVData]
    val wfwrite: DataWriter[Iterable[CSVData]] = CSVFileWriter[CSVData]("fakefile.csv", ',')

    assert(wfread.isInstanceOf[Workflow[_,_]])
    assert(trans.isInstanceOf[Workflow[_,_]])
    assert(wfwrite.isInstanceOf[Workflow[_,_]])

    val wseq = WorkSequence(wfread, trans) ++ wfwrite

    assert(wseq.isInstanceOf[WorkSequence[_,_,_]])
    assert(wseq.steps.map(_.getClass.getName) == List(
      "de.tao.soda.etl.data.CSVFileReader",
      "de.tao.soda.etl.IteratorToIterable",
      "de.tao.soda.etl.data.CSVFileWriter"))
  }

  it should "append a WorkSequence with another WorkSequence" in {
    val wfread: DataReader[Iterator[CSVData]] = CSVFileReader[CSVData](',')
    val trans = new IteratorToIterable[CSVData]
    val wseq1 = WorkSequence(wfread, trans)

    implicit val rfc: RawFieldsConverter[B1]
    val f: Function[CSVData, B1] = { _ => throw new NotImplementedException("")}
    val mapper = new MapIter[CSVData, B1](f)
    val writer = new CSVFileWriter[B1]("foo.tsv", '\t')
    val wseq2 = WorkSequence(mapper, writer)

    // Join both sequences
    val joined = wseq1 ++ wseq2
    assert(joined.isInstanceOf[WorkSequence[_,_,_]])
    assert(joined.steps.map(_.getClass.getName) == List(
      "de.tao.soda.etl.data.CSVFileReader",
      "de.tao.soda.etl.IteratorToIterable", 
      "de.tao.soda.etl.workflow.MapIter",
      "de.tao.soda.etl.data.CSVFileWriter"
    ))
  }

}
