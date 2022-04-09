package de.tao.soda.etl

trait DataDescriptor[T] {
  def isOverwriting: Boolean

  def read[T]: T
  def write[T]: T
  def show[T]: Unit
}

trait DataReader[T] extends Workflow[DataDescriptor[T], T]
trait DataWriter[T] extends Workflow[T, DataDescriptor[T]]
trait DataIntercept[T] extends Workflow[DataDescriptor[T], DataDescriptor[T]] {
  def intercept(data: DataDescriptor[T])
  override def run(input: DataDescriptor[T], dry: Boolean): DataDescriptor[T] = {
    intercept(input)
    input
  }
}
