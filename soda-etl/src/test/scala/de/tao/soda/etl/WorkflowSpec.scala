package de.tao.soda.etl

import de.tao.soda.etl.workflow.{MapIter, MapWithWorkflow, Mapper}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowSpec extends AnyFlatSpec with BeforeAndAfter {

  case class T(a: Int, b: Option[String], c: Seq[Double])
  case class M(a: String, c: Double)

  val map: (T => M) = (t => M(t.b.map(_ + t.a.toString).getOrElse(""), t.c.sum))

  lazy val inputList = List(
    T(1, None, Nil),
    T(1, Some(""), Seq(1,2)),
    T(0, Some("c"), Seq(3,5)),
    T(0, Some("abc"), Seq(1,10))
  )

  it should "map" in {
    val m = new Mapper[T,M](map, null)
    val inp = T(1, None, Nil)
    assert(m.run(inp) == M("", 0))
  }

  it should "mapIter" in {
    val m = new MapIter[T,M](map)
    val out = m.run(inputList)
    assert(out.size == inputList.size)
    assert(out.toList == List(
      M("", 0),
      M("1", 3),
      M("c0", 8),
      M("abc0", 11)
    ))
  }

  it should "mapWithWorkflow" in {
    val wf = new Mapper[T,M](map, null)
    val m = new MapWithWorkflow[T,M](wf)
    val out = m.run(inputList)
    assert(out.size == inputList.size)
    assert(out.toList == List(
      M("", 0),
      M("1", 3),
      M("c0", 8),
      M("abc0", 11)
    ))
  }
}
