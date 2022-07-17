package de.tao.soda.etl

import de.tao.soda.etl.data.{ReadCSV, WriteAsCSV, WriteAsJSON, ReadZippedAsObject, OutputPath}
import de.tao.soda.etl.workflow.{Intercept, InterceptOutput, InterceptToBinaryFile, InterceptToCSV, InterceptToJSON, MapIter, Mapper, WorkSequence}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import Domain._
import de.tao.soda.etl.data.OutputIdentifier.$
import org.apache.commons.lang.NotImplementedException
import purecsv.unsafe.converter.RawFieldsConverter

class WorkSequenceSpec extends AnyFlatSpec with BeforeAndAfter {

  it should "connect workflows together" in {
    val wfread: DataReader[Iterator[CSVData]] = ReadCSV[CSVData](',')
    val trans = new ToIterable[CSVData]
    val wfwrite: DataWriter[Iterable[CSVData]] = WriteAsCSV[CSVData]($("fakefile.csv"), ',')

    assert(wfread.isInstanceOf[Workflow[_,_]])
    assert(trans.isInstanceOf[Workflow[_,_]])
    assert(wfwrite.isInstanceOf[Workflow[_,_]])

    val wseq = WorkSequence(wfread, trans) ++ wfwrite

    assert(wseq.isInstanceOf[WorkSequence[_,_,_]])
    assert(wseq.steps.map(_.getClass.getName) == List(
      "de.tao.soda.etl.data.ReadCSV",
      "de.tao.soda.etl.ToIterable",
      "de.tao.soda.etl.data.WriteAsCSV"))
  }

  it should "append a WorkSequence with another WorkSequence" in {
    val wfread: DataReader[Iterator[CSVData]] = ReadCSV[CSVData](',')
    val trans = new ToIterable[CSVData]
    val wseq1 = WorkSequence(wfread, trans)

    implicit val rfc: RawFieldsConverter[B1]
    val f: Function[CSVData, B1] = { _ => throw new NotImplementedException("")}
    val mapper = new MapIter[CSVData, B1](f)
    val writer = new WriteAsCSV[B1]($("foo.tsv"), '\t')
    val wseq2 = WorkSequence(mapper, writer)

    // Join both sequences
    val joined = wseq1 ++ wseq2
    assert(joined.isInstanceOf[WorkSequence[_,_,_]])
    assert(joined.steps.map(_.getClass.getName) == List(
      "de.tao.soda.etl.data.ReadCSV",
      "de.tao.soda.etl.ToIterable",
      "de.tao.soda.etl.workflow.MapIter",
      "de.tao.soda.etl.data.WriteAsCSV"
    ))

    assert(joined.printTree() ==
      """
        |+--ReadCSV
        |+--ToIterable
        |+--MapIter
        |+--WriteAsCSV
        |""".stripMargin.tail.stripLineEnd)
  }

  it should "build complex WorkSequence with Mux" in {
    implicit val rfc: RawFieldsConverter[B1] = null
    implicit val klazz = classOf[JSONData]
    implicit val klazz2 = classOf[B1]
    implicit val klazz3 = classOf[Option[B1]]

    val step1 = new ReadZippedAsObject[JSONData]()
    val step2 = new InterceptOutput[JSONData](WriteAsJSON[JSONData]($("filename.json")))
    val step3 = new Intercept[JSONData, JSONData, Option[B1]](new IdentityWorkflow[JSONData], {
      val w1 = new Mapper[JSONData, B1]((data: JSONData) => data.body)
      val w2 = new InterceptToBinaryFile[B1]($("filename.bin"))
      val w3 = new LiftOption[B1]()
      val w4 = new InterceptToJSON[Option[B1]](filename=$("filename.json"))
      val ws1: WorkSequence[JSONData, B1, B1] = new WorkSequence(w1, w2)
      val ws2: WorkSequence[B1, Option[B1], Option[B1]] = new WorkSequence(w3, w4)
      ws1 ++ ws2
    })

    val wseq = WorkSequence(step1, step2) ++ step3

    assert(wseq.printTree() ==
      """
      |+--ReadZippedAsObject
      |+--InterceptOutput
      ||  +--[self]
      ||  |  +--IdentityWorkflow
      ||  +--[plex]
      ||  |  +--WriteAsJSON
      |+--Intercept
      ||  +--[self]
      ||  |  +--IdentityWorkflow
      ||  +--[plex]
      ||  |  +--Mapper
      ||  |  +--InterceptToBinaryFile
      ||  |  |  +--[self]
      ||  |  |  |  +--IdentityWorkflow
      ||  |  |  +--[plex]
      ||  |  |  |  +--WriteAsObject
      ||  |  +--LiftOption
      ||  |  +--InterceptToJSON
      ||  |  |  +--[self]
      ||  |  |  |  +--IdentityWorkflow
      ||  |  |  +--[plex]
      ||  |  |  |  +--WriteAsJSON
      |""".stripMargin.tail.stripLineEnd)

  }

}