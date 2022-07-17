package de.tao.soda.runnable

import com.amazonaws.regions.Regions
import com.typesafe.scalalogging.LazyLogging
import de.tao.soda.Domain.{PolishedRow, SimpleRow}
import de.tao.soda.etl.data.{WriteIteratorAsCSV, ReadCSV, OutputPath, ReadS3ObjectAsStream, TimestampPath}
import de.tao.soda.etl.workflow.{FilterIterator, MapIterator}

import java.time.format.DateTimeFormatter

class Polisher extends MapIterator[SimpleRow, PolishedRow]((s: SimpleRow) => PolishedRow(
  a = s.a,
  s = s.s,
  value = scala.util.Random.nextDouble()*10))

object DownloadS3 extends App with LazyLogging {

  val bucket = "soda-test-123"

  lazy val workflow = new ReadS3ObjectAsStream(bucket, "utf-8", Regions.EU_CENTRAL_1) +>
    new ReadCSV[SimpleRow](',') +>
    new Polisher() +>
    new FilterIterator[PolishedRow](_.value > 0) +>
    new WriteIteratorAsCSV[PolishedRow](OutputPath("soda-polished.csv"), ',')

  val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")
  workflow.run(TimestampPath("soda-test", fmt, ".csv").toString)
}
