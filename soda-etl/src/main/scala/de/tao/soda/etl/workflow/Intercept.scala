package de.tao.soda.etl.workflow

import de.tao.soda.etl.data.{WriteAsCSV, WriteAsJSON, WriteAsObject, OutputIdentifier}
import de.tao.soda.etl.{DataWriter, IdentityWorkflow, InputIdentifier, IsoWorkflow, Multiplexer, Workflow}
import purecsv.unsafe.converter.RawFieldsConverter

class Intercept[T0,T1,T2](override val self: Workflow[T0,T1], override val plex: Workflow[T0, T2]) extends Multiplexer[T0, T1, T2]

class IsoIntercept[T](override val self: IsoWorkflow[T], override val plex: IsoWorkflow[T]) extends Intercept[T,T,T](self, plex)

// Intercept single object
class InterceptOutput[T <: Product with Serializable]
  (intercept: DataWriter[T])
  extends Multiplexer[T, T, InputIdentifier]{
  override val self: Workflow[T, T] = new IdentityWorkflow[T]
  override val plex: Workflow[T, InputIdentifier] = intercept
}

// Intercept iterable object
class InterceptIterOutput[T <: Product with Serializable]
(intercept: DataWriter[Iterable[T]])
  extends Multiplexer[Iterable[T], Iterable[T], InputIdentifier]{
  override val self: Workflow[Iterable[T], Iterable[T]] = new IdentityWorkflow[Iterable[T]]
  override val plex: Workflow[Iterable[T], InputIdentifier] = intercept
}

final class InterceptToJSON[T <: Product with Serializable](filename: OutputIdentifier)(implicit clazz: Class[T])
  extends InterceptOutput[T](intercept = WriteAsJSON[T](filename)(clazz))

final class InterceptToBinaryFile[T <: Product with Serializable](filename: OutputIdentifier)
  extends InterceptOutput[T](intercept = WriteAsObject[T](filename))

// Only for iterables
final class InterceptToCSV[T <: Product with Serializable](filename: OutputIdentifier, delimiter: Char)
  (implicit val rc: RawFieldsConverter[T])
  extends InterceptIterOutput[T](intercept = WriteAsCSV[T](filename, delimiter)(rc))
