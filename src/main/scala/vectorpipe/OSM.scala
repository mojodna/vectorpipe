package vectorpipe

import java.sql.Timestamp

import org.locationtech.jts.{geom => jts}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import vectorpipe.functions.osm.removeUninterestingTags
import vectorpipe.internal._

object OSM {
  /**
    * Convert a raw OSM DataFrame into a frame containing JTS geometries for each unique id/changeset.
    *
    * This currently produces Points for nodes containing "interesting" tags, LineStrings and Polygons for ways
    * (according to OSM rules for defining areas), MultiPolygons for multipolygon and boundary relations, and
    * LineStrings / MultiLineStrings for route relations.
    *
    * @param input DataFrame containing node, way, and relation elements
    * @return DataFrame containing geometries.
    */
  def toGeometry(input: DataFrame): DataFrame = {
    import input.sparkSession.implicits._

    val st_pointToGeom = org.apache.spark.sql.functions.udf { pt: jts.Point => pt.asInstanceOf[jts.Geometry] }

    val elements = input
      .withColumn("tags", removeUninterestingTags('tags))

    val nodes = preprocessNodes(elements)
    val ways = preprocessWays(elements)
    val relations = preprocessRelations(elements)

    val nodeGeoms = constructPointGeometries(nodes)
      .withColumn("minorVersion", lit(0))
      .withColumn("geom", st_pointToGeom('geom))

    // way geometries (including untagged ways which may be part of relations; standalone untagged ways will be filtered
    // out later)
    val wayGeoms = reconstructWayGeometries(ways, nodes)
      // used more than once
      .localCheckpoint()

    val members = relations
      .select(explode('members) as 'member)
      .select('member.getField("type") as '_type, 'member.getField("ref") as 'ref)
      .distinct
      .localCheckpoint()

    // nodes that are members of relations (and need to be turned into Point geometries)
    val nodesForRelations = members
      .where('_type === NodeType)
      .select('ref as 'id)
      .distinct
      .join(nodes, Seq("id"))

    // generate Point geometries for all node members of relations (this is separate from nodeGeoms, as that only
    // contains interestingly-tagged nodes and relations may refer to untagged nodes)
    val relationNodeGeometries = constructPointGeometries(nodesForRelations, all = true)
      .withColumn("minorVersion", lit(0))
      .withColumn("geom", st_pointToGeom('geom))
      .withColumn("geometryChanged", lit(true))

    val relationWayGeometries = members
      .where('_type === WayType)
      .select('ref as 'id)
      .distinct
      .join(wayGeoms, Seq("id"))
      .select(
        '_type,
        'id,
        'geom,
        'tags,
        'changeset,
        'updated,
        'validUntil,
        'visible,
        'version,
        'minorVersion,
        'geometryChanged
      )

    // when geometries are duplicated (tagged ways AND relations they're part of), they can be unioned according to
    // geometries (and have their tags merged (preferring relation tags) -- or drop non-relations? or drop non-relations
    // only when tags fully match?)

    val relationGeoms = reconstructRelationGeometries(elements, relationNodeGeometries.union(relationWayGeometries))

    nodeGeoms
      .union(wayGeoms.where(size('tags) > 0).drop('geometryChanged))
      .union(relationGeoms)
  }

  /**
    * Snapshot pre-processed elements.
    *
    * A Time Pin is stuck through a set of elements that have been augmented with a 'validUntil column to identify all
    * that were valid at a specific point in time (i.e. updated before the target timestamp and valid after it).
    *
    * @param df        Elements (including 'validUntil column)
    * @param timestamp Optional timestamp to snapshot at
    * @return DataFrame containing valid elements at timestamp (or now)
    */
  def snapshot(df: DataFrame, timestamp: Timestamp = null): DataFrame = {
    import df.sparkSession.implicits._

    df
      .where(
        'updated <= coalesce(lit(timestamp), current_timestamp)
          and coalesce(lit(timestamp), current_timestamp) < coalesce('validUntil, date_add(current_timestamp, 1)))
  }

  /**
    * Augment geometries with user metadata.
    *
    * When 'changeset is included, user (name and 'uid) metadata is joined from a DataFrame containing changeset
    * metadata.
    *
    * @param geoms      Geometries to augment.
    * @param changesets Changesets DataFrame with user metadata.
    * @return Geometries augmented with user metadata.
    */
  def addUserMetadata(geoms: DataFrame, changesets: DataFrame): DataFrame = {
    import geoms.sparkSession.implicits._

    geoms
      .join(changesets.select('id as 'changeset, 'uid, 'user), Seq("changeset"))
  }

}
