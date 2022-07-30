package de.tao.soda.runnable

import com.amazonaws.regions.Regions
import com.typesafe.scalalogging.LazyLogging
import de.tao.soda.Domain._
import de.tao.soda.etl.data.{WriteAsJSON, ReadS3ZippedObject, WriteS3ZippedObject, UUIDPath}
import de.tao.soda.etl.workflow.MapWithWorkflow
import de.tao.soda.etl.{Generator, InputIdentifier, InputToString}

class GenerateBrackets(num: Int) extends Generator[Iterable[Bracket]]{
  override def run(input: Unit): Iterable[Bracket] = {
    (1 to num).map{i => Bracket(
      java.util.UUID.randomUUID().toString,
      Array.fill(50)(SubBracket(i, java.util.UUID.randomUUID().toString, Array.fill(1000)(scala.util.Random.nextDouble()))),
      Array.fill(100)(Event(java.util.UUID.randomUUID().toString, java.time.LocalDate.now().toString, scala.util.Random.nextDouble()))
    )}
  }
}

object UploadZippedS3 extends App with LazyLogging {
  val bucket = "soda-test-brakets"
  val num = 5

  implicit val clz = classOf[Bracket]
  val uidZipNameGen = UUIDPath("soda", ".gz")
  val uidJsonNameGen = UUIDPath("soda-fromzip-", ".json")

  lazy val subWorkflow = new WriteS3ZippedObject[Bracket](bucket, uidZipNameGen, Regions.EU_CENTRAL_1) +>
    InputToString +>
    new ReadS3ZippedObject[Bracket](bucket, Regions.EU_CENTRAL_1) +>
    new WriteAsJSON[Bracket](uidJsonNameGen)

  lazy val workflow = new GenerateBrackets(num) +> new MapWithWorkflow[Bracket, InputIdentifier](subWorkflow)

  logger.info("[UploadZippedS3] app starting")
  logger.info("\n" + workflow.printTree())
  workflow.run()
  workflow.shutdownHook()
  logger.info("[UploadZippedS3] app ending")
}