package de.tao.soda.etl.workflow

import de.tao.soda.etl.{IdentityWorkflow, Multiplexer, Workflow}

/**
 * Work sequence executes in order
 */
case class WorkSequence[T0, T1, T2](head: Workflow[T0, T1], next: Workflow[T1, T2]) extends Workflow[T0, T2]{
  override def run(input: T0, dry: Boolean): T2 = {
    if (dry)
      logger.info(s"WorkSequence to run ${head.getClass.getName} followed by ${next.getClass.getName}")
    else
      logger.info(s"WorkSequence running ${head.getClass.getName} followed by ${next.getClass.getName}")

    next.run(head.run(input, dry), dry)
  }

  final def steps: List[Workflow[_,_]] = {
    head :: (next match {
      case w : IdentityWorkflow[_] => Nil
      case w @ WorkSequence(_, _) => w.steps
      case _ => next :: Nil
    })
  }

  // append another workflow
  def ++[T3](tail: Workflow[T2, T3]): WorkSequence[T0, T1, T3] = {
    val newNext: Workflow[T1, T3] = next match {
      case w: WorkSequence[_,_,_] => w.copy(next = WorkSequence.join(w.next, tail))
      case w: Workflow[_,_] => WorkSequence(w, tail)
    }
    WorkSequence[T0, T1, T3](head, newNext)
  }

  override def printTree(level: Int=0): String = {
    steps.map(_.printTree(level)).mkString("\n")
  }
}

object WorkSequence {

  def apply[T0, T1](head: Workflow[T0, T1]): WorkSequence[T0, T1, T1] =
    WorkSequence(head, new IdentityWorkflow[T1])

  def join[T0, T1, T2](head: Workflow[T0, T1], tail: Workflow[T1, T2]):Workflow[T0, T2] = head match {
    case w: WorkSequence[T0,_,T1] => w ++ tail
    case _ => WorkSequence(head, tail)
  }
}
