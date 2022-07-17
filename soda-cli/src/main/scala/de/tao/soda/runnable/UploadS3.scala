package de.tao.soda.runnable

import com.amazonaws.regions.Regions
import com.typesafe.scalalogging.LazyLogging
import de.tao.soda.Domain.SimpleRow
import de.tao.soda.etl.Generator
import de.tao.soda.etl.data.{WriteAsCSV, S3Uploader, TimestampPath}

import java.time.format.DateTimeFormatter
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

  val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")
  lazy val workflow = new GenerateList(num) +>
    new WriteAsCSV[SimpleRow](TimestampPath("soda-test", fmt, ".csv"), ',') +>
    new S3Uploader(bucket, TimestampPath("soda-test", fmt, ".csv"), Regions.EU_CENTRAL_1)

  logger.info("[UploadS3] app starting")
  logger.info("\n" + workflow.printTree())
  workflow.run()
  logger.info("[UploadS3] app ending")
}
