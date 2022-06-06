package de.tao.soda.etl.workflow

import de.tao.soda.etl.Workflow

/**
 * MapIter maps an iterable with a mapping function
 * @param mapper
 * @tparam T
 */
class MapIter[T1, T2](mapper: T1 => T2) extends Workflow[Iterable[T1], Iterable[T2]] {

  override def run(input: Iterable[T1], dry: Boolean): Iterable[T2] = {
    if (dry){
      logger.info(s"MapIter will be executing with : ${mapper}")
      Iterable.empty[T2]
    }
    else{
      logger.info(s"MapIter is executing with : ${mapper}")
      input.map(mapper)
    }
  }
}

class Mapper[T1, T2](mapper: T1 => T2, default: T2) extends Workflow[T1, T2] {
  override def run(input: T1, dry: Boolean): T2 = {
    if (dry){
      logger.info(s"Mapper will be executing with : ${mapper}")
      default
    }
    else{
      logger.info(s"Mapper is executing with : ${mapper}")
      mapper(input)
    }
  }
}

/**
 * MapWithWorkflow maps an iterable into another iterable with a mapping function
 * @param workflow
 * @tparam T1
 * @tparam T2
 */
class MapWithWorkflow[T1, T2](workflow: Workflow[T1, T2]) extends Workflow[Iterable[T1], Iterable[T2]] {

  override def run(input: Iterable[T1], dry: Boolean): Iterable[T2] = {
    val word = if (dry) "will be" else "is"
    logger.info(s"MapWithWorkflow ${word} executing with : ${workflow.getClass.getName}")
    input.map(workflow.run(_, dry))
  }
}
