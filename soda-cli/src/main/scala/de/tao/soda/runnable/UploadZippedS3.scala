package de.tao.soda.runnable

import com.amazonaws.regions.Regions
import com.typesafe.scalalogging.LazyLogging
import de.tao.soda.Domain._
import de.tao.soda.etl.{Generator, InputIdentifier, InputToString}
import de.tao.soda.etl.data.{JSONFileWriter, S3ObjectReader, S3Writer, S3ZippedObjectReader, S3ZippedWriter, UUIDPath}
import de.tao.soda.etl.workflow.MapWithWorkflow
import de.tao.soda.runnable.UploadS3.{logger, workflow}

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
  val num = 20

  implicit val clz = classOf[Bracket]
  val uidNameGen = UUIDPath("soda", ".gz")

  lazy val subWorkflow = new S3ZippedWriter[Bracket](bucket, uidNameGen, Regions.EU_CENTRAL_1) +>
    InputToString +>
    new S3ZippedObjectReader[Bracket](bucket, Regions.EU_CENTRAL_1) +>
    new JSONFileWriter[Bracket](uidNameGen)

  lazy val workflow = new GenerateBrackets(num) +> new MapWithWorkflow[Bracket, InputIdentifier](subWorkflow)

  logger.info("[UploadZippedS3] app starting")
  logger.info("\n" + workflow.printTree())
  workflow.run()
  logger.info("[UploadZippedS3] app ending")
}