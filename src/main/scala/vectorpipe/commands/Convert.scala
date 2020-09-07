package vectorpipe.commands

import java.time.{Duration, Instant}

import cats.implicits._
import com.monovore.decline._
import geotrellis.spark.store.kryo.KryoRegistrator
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts._
import vectorpipe.OSM._
import vectorpipe.functions.box2d

/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class vectorpipe.commands.Convert \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --input $HOME/data/osm/isle-of-man.orc \
 *   --output $HOME/data/osm/isle-of-man-geoms/
 */

object Convert
  extends CommandApp(
    name = "convert",
    header = "Create geometries from OSM-style input",
    main = {

      /* CLI option handling */
      val orcO = Opts.option[String]("input", short = "i", help = "Location of the ORC file to process")
      val outO = Opts.option[String]("output", short = "o", help = "ORC file containing geometries")
      val numPartitionsO =
        Opts.option[Int]("partitions", help = "Number of partitions to generate").withDefault(1)

      (orcO, outO, numPartitionsO).mapN {
        (orc, out, numPartitions) =>
          /* Settings compatible for both local and EMR execution */
          val conf = new SparkConf()
            .setIfMissing("spark.master", "local[*]")
            .setAppName("convert")
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
            .set("spark.sql.orc.impl", "native")
            .set("spark.sql.orc.filterPushdown", "true")
            .set("spark.ui.showConsoleProgress", "true")

          implicit val spark: SparkSession = SparkSession.builder
            .config(conf)
            .getOrCreate
          import spark.implicits._
          spark.withJTS

          Logger.getRootLogger.setLevel(Level.WARN)

          val start = Instant.now
          val df = spark.read.orc(orc)

          toGeometry(df)
            // create a bbox struct to optimize spatial filtering
            .withColumn("bbox", box2d('geom))
            // store the geometry as WKB
            // .withColumn("geom", st_asBinary('geom))
            // store the geometry as WKT
            .withColumn("geom", when(not(st_isEmpty('geom)), st_asText('geom)))
            .repartition(numPartitions)
            .write
            .mode(SaveMode.Overwrite)
            .orc(out)

          spark.stop()

          println(s"Done: ${Duration.between(start, Instant.now()).getSeconds}.")
      }
    }
  )
