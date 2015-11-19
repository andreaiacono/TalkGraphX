package graphx

import graphstream.SimpleGraphViewer
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object PregelSample extends App {

  val vertices = US_CITIES_VERTICES_FILENAME
  val edges = US_CITIES_EDGES_FILENAME

  // launches the viewer of the graph
  new SimpleGraphViewer(vertices, edges, false).run()

  // loads the graph
  val sparkContext = getSparkContext()
  val graph: Graph[String, Double] = loadCitiesGraphFromFiles(sparkContext, vertices, edges)

  // launches pregel computation
  shortestPath(sparkContext, graph)

  def shortestPath(sc: SparkContext, graph: Graph[String, Double]) = {

    // we want to know the shortest paths from this vertex to all the others vertices
    // vertexId 1 is the city of Arad
    val sourceCityId: VertexId = 1L

    // initialize the graph such that all vertices except the root have distance infinity
    val initialGraph: Graph[VertexAttribute, Double] =
      graph.mapVertices((vertexId, cityName) =>
        if (vertexId == sourceCityId)
          new VertexAttribute(
            cityName,
            0.0,
            List[City](new City(cityName, sourceCityId))
          )
        else
          new VertexAttribute(
            cityName,
            Double.PositiveInfinity,
            List[City]())
      )

    // applies the pregel computation to the graph
    val shortestPathGraph = initialGraph.pregel(
      initialMsg = new VertexAttribute("", Double.PositiveInfinity, List[City]()),
      maxIterations = Int.MaxValue,
      activeDirection = EdgeDirection.Out
    ) (

      // vprog
      (vertexId, currentAttr, newAttr) =>
        if (currentAttr.distance <= newAttr.distance) currentAttr else newAttr,

      // sendMsg
      edgeTriplet => {
        if (edgeTriplet.srcAttr.distance < (edgeTriplet.dstAttr.distance - edgeTriplet.attr)) {
          Iterator(
              ( edgeTriplet.dstId,
                new VertexAttribute(
                  edgeTriplet.dstAttr.cityName,
                  edgeTriplet.srcAttr.distance + edgeTriplet.attr,
                  edgeTriplet.srcAttr.path :+ new City(edgeTriplet.dstAttr.cityName, edgeTriplet.dstId )
              )
            )
          )
        }
        else {
          Iterator.empty
        }
      },

      // mergeMsg
      (attribute1, attribute2) =>
        if (attribute1.distance < attribute2.distance) {
          attribute1
        }
        else {
          attribute2
        }

    )

    for ((destVertexId, attribute) <- shortestPathGraph.vertices) {
      println(s"Going from Washington to ${attribute.cityName} " +
              s"has a distance of ${attribute.distance} km. " +
              s"Path is: ${attribute.path.mkString(" => ")}")
    }

  }

}
