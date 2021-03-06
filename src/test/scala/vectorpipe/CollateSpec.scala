package vectorpipe

import java.time.Instant

import geotrellis.vector._
import org.scalatest._
import vectorpipe.osm._

// --- //

class CollateSpec extends FunSpec with Matchers {
  val extent: Extent = Extent(0, 0, 1000, 1000)

  val geom: Polygon = Polygon(
    exterior = Line((1,1), (8,1), (8,4), (1,4), (1,1)),
    holes = Seq(
      Line((2,2), (2,3), (3,3), (3,2), (2,2)),
      Line((4,2), (4,3), (5,3), (5,2), (4,2)),
      Line((6,2), (6,3), (7,3), (7,2), (6,2))
    )
  )

  val data0 = ElementMeta(
    1037, "colin", 8765, 5, 1, Instant.now, true, Map("object" -> "flagpole")
  )

  val data1 = ElementMeta(
    10000, "colin", 8765, 5, 1, Instant.now, true, Map("route" -> "footpath")
  )

  val data2 = ElementMeta(
    100123, "colin", 8765, 5, 1, Instant.now, true, Map("place" -> "park")
  )

  describe("Collation Functions") {
    it("withoutMetadata") {
      Collate.withoutMetadata(extent, Seq(Feature(geom, 1)))
    }

    it("byOSM") {
      Collate.byOSM(extent, Seq(Feature(geom, data0)))
    }
  }
}
