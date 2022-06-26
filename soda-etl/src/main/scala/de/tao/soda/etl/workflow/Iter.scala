package de.tao.soda.etl.workflow

import de.tao.soda.etl.Workflow

class Reducer[T](red: (T, T) => T) extends Workflow[Iterable[T], T] {
  override def run(input: Iterable[T], dry: Boolean): T = {
    if (dry){
      logger.info(s"Reducer to run on iterable")
      input.head
    }
    else {
      logger.info(s"Reducer running on iterable")
      input.reduce(red)
    }
  }
}

class ReduceIterator[T](red: (T, T) => T) extends Workflow[Iterator[T], T] {
  override def run(input: Iterator[T], dry: Boolean): T = {
    if (dry){
      logger.info(s"Reducer to run on iterator")
      input.next()
    }
    else {
      logger.info(s"Reducer running on iterator")
      input.reduce(red)
    }
  }
}

class Filter[T](f: (T => Boolean)) extends Workflow[Iterable[T], Iterable[T]] {
  override def run(input: Iterable[T], dry: Boolean): Iterable[T] = {
    if (dry){
      logger.info(s"Filter to run on iterable")
      input
    }
    else {
      val filtered = input.filter(f)
      val nBefore = input.size
      val nAfter = filtered.size
      val diff = nBefore - nBefore
      logger.info(s"Filter running on iterable : $nBefore records -> $nAfter records ($diff removed)")
      filtered
    }
  }
}

class FilterIterator[T](f: (T => Boolean)) extends Workflow[Iterator[T], Iterator[T]] {
  override def run(input: Iterator[T], dry: Boolean): Iterator[T] = {
    if (dry){
      logger.info(s"Filter to run on Iterator")
      input
    }
    else {
      val filtered = input.filter(f)
      logger.info(s"Filter running on iterator")
      filtered
    }
  }
}

class GroupAgg[T, K, B](key: (T => K), mapV: Iterable[T] => B) extends Workflow[Iterable[T], Iterable[(K,B)]] {
  override def run(input: Iterable[T], dry: Boolean): Iterable[(K, B)] = {
    if (dry){
      logger.info(s"GroupAgg to run")
      Iterable.empty
    }
    else {
      logger.info(s"GroupAgg running")
      input.groupBy(key).view.mapValues(mapV)
    }
  }
}


