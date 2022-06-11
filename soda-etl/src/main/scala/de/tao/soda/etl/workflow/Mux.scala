package de.tao.soda.etl.workflow

import de.tao.soda.etl.{IdentityWorkflow, IsoWorkflow, Multiplexer, Workflow}

abstract class Mux [T0 <: Product with Serializable, T1 <: Product with Serializable, T2 <: Product with Serializable]
  (override val self: Workflow[T0, T1], override val plex: Workflow[T0, T2])
  extends Multiplexer[T0, T1, T2]

class MirrorMux [T0 <: Product with Serializable, T1 <: Product with Serializable]
  (override val plex: Workflow[T0, T1])
  extends Mux[T0, T0, T1]( new IdentityWorkflow[T0], plex)

class IsoMirrorMux [T <: Product with Serializable](override val plex: IsoWorkflow[T])
  extends MirrorMux[T, T](plex)

