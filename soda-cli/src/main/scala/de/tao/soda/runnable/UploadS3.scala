package de.tao.soda.runnable

import com.amazonaws.regions.Regions
import com.typesafe.scalalogging.LazyLogging
import de.tao.soda.Domain.SimpleRow
import de.tao.soda.etl.Generator
import de.tao.soda.etl.data.{TimestampPath, UploadToS3, WriteAsCSV}
import org.joda.time.DateTime

import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date, UUID}

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
//  val now = new DateTime(Calendar.getInstance.getTime) // java date -> joda
//  val expiry = Some(now.plusMinutes(10).toDate) // joda -> java date
  lazy val workflow = new GenerateList(num) +>
    new WriteAsCSV[SimpleRow](TimestampPath("soda-test", fmt, ".csv"), ',') +>
    new UploadToS3(bucket, TimestampPath("soda-test", fmt, ".csv"), Regions.EU_CENTRAL_1)

  logger.info("[UploadS3] app starting")
  logger.info("\n" + workflow.printTree())
  workflow.run()
  workflow.shutdownHook()
  logger.info("[UploadS3] app ending")
}
