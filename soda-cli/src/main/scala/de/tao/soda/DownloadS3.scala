package de.tao.soda

import com.amazonaws.regions.Regions
import com.typesafe.scalalogging.LazyLogging
import de.tao.soda.etl.data.{CSVFileIteratorWriter, CSVFileReader, CSVFileWriter, S3ObjectReader, S3StreamReader}
import de.tao.soda.etl.workflow.{Filter, FilterIterator, MapIter, MapIterator}
import Domain._

class Polisher extends MapIterator[SimpleRow, PolishedRow]((s: SimpleRow) => PolishedRow(
  a = s.a,
  s = s.s,
  value = scala.util.Random.nextDouble()*10))

object DownloadS3 extends App with LazyLogging {

  val bucket = "soda-test-123"
  val s3objectName = "mycsv.csv"

  lazy val workflow = new S3StreamReader(bucket, "utf-8", Regions.EU_CENTRAL_1) +>
    new CSVFileReader[SimpleRow](',') +>
    new Polisher() +>
    new FilterIterator[PolishedRow](_.value > 0) +>
    new CSVFileIteratorWriter[PolishedRow]("soda-polished.csv", ',')

  workflow.run(s3objectName)
}
