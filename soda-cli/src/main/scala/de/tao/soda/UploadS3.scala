package de.tao.soda

import com.amazonaws.regions.Regions
import com.typesafe.scalalogging.LazyLogging
import de.tao.soda.etl.Generator
import de.tao.soda.etl.data.{CSVFileWriter, S3Uploader}
import Domain._

import java.util.UUID

class GenerateList(num: Int) extends Generator[Iterable[SimpleRow]]{
  override def run(input: Unit): Iterable[SimpleRow] = {
    (1 to num).map{ i =>
      SimpleRow(i, UUID.randomUUID().toString)
    }
  }
}

object UploadS3 extends App with LazyLogging {
  val bucket = "soda-test-123"
  val num = 100

  lazy val workflow = new GenerateList(num) +>
    new CSVFileWriter[SimpleRow]("soda-test.csv", ',') +>
    new S3Uploader(bucket, "mycsv.csv", Regions.EU_CENTRAL_1)

  logger.info("[UploadS3] app starting")
  logger.info("\n" + workflow.printTree())
  workflow.run()
  logger.info("[UploadS3] app ending")
}
