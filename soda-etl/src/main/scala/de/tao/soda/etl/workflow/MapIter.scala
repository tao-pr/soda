package de.tao.soda.etl.workflow

import de.tao.soda.etl.Workflow

/**
 * MapIter maps an iterable with a mapping function
 * @param mapper
 * @tparam T
 */
class MapIter[T1, T2](mapper: T1 => T2) extends Workflow[Iterable[T1], Iterable[T2]] {

  override def run(input: Iterable[T1]): Iterable[T2] = {
    logger.info(s"MapIter is executing with : ${mapper}")
    input.map(mapper)
  }
}

class MapIterator[T1, T2](mapper: T1 => T2) extends Workflow[Iterator[T1], Iterator[T2]] {

  override def run(input: Iterator[T1]): Iterator[T2] = {
    logger.info(s"MapIterator is executing with : ${mapper}")
    input.map(mapper)
  }
}

class Mapper[T1, T2](mapper: T1 => T2) extends Workflow[T1, T2] {
  override def run(input: T1): T2 = {
    logger.info(s"Mapper is executing with : ${mapper}")
    mapper(input)
  }
}

/**
 * MapWithWorkflow maps an iterable into another iterable with a mapping function
 * @param workflow
 * @tparam T1
 * @tparam T2
 */
class MapWithWorkflow[T1, T2](workflow: Workflow[T1, T2]) extends Workflow[Iterable[T1], Iterable[T2]] {

  override def run(input: Iterable[T1]): Iterable[T2] = {
    logger.info(s"MapWithWorkflow is executing with : ${workflow.getClass.getName}")
    input.map(workflow.run(_))
  }
}
