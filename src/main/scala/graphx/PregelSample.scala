package graphx

import graphstream.SimpleGraphViewer
import graphx.types.{City, VertexAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object PregelSample extends App {

  val vertices = CITIES_VERTICES_FILENAME
  val edges = CITIES_EDGES_FILENAME

  // launches the viewer of the graph
  new SimpleGraphViewer(vertices, edges, false).run()

  // loads the graph
  val sparkContext = getSparkContext()
  val graph = loadCitiesGraphFromFiles(sparkContext, vertices, edges)

  // launches pregel computation for shortest path
  shortestPath(sparkContext, graph)

  /**
    * Pregel implementation of Dijkstra algorithm for shortest path:
    * https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm
    * @param sparkContext
    * @param graph
    */
  def shortestPath(sparkContext: SparkContext, graph: Graph[String, Double]) = {

    // we want to know the shortest paths from the vertex 1 (city of Arad)
    // to all the others vertices (all the other cities)
    val sourceCityId: VertexId = 1L

    // initialize a new graph with data of the old one, plus distance and path;
    // for all vertices except the city we want to compute the path from, the
    // distance is set to infinity
    val initialGraph: Graph[VertexAttribute, Double] = graph.mapVertices(
      (vertexId, cityName) =>
        if (vertexId == sourceCityId) {
          VertexAttribute(
            cityName,
            0.0,
            List[City](City(cityName, sourceCityId))
          )
        }
        else {
          VertexAttribute(
            cityName,
            Double.PositiveInfinity,
            List[City]())
        }
      )

    // calls pregel
    val shortestPathFunction = initialGraph.pregel(
      initialMsg = VertexAttribute("", Double.PositiveInfinity, List[City]()),
      maxIterations = Int.MaxValue,
      activeDirection = EdgeDirection.Out
    ) (

      // vprog
      (vertexId, currentVertexAttr, newVertexAttr) =>
        if (currentVertexAttr.distance <= newVertexAttr.distance) currentVertexAttr else newVertexAttr,

      // sendMsg
      edgeTriplet => {
        if (edgeTriplet.srcAttr.distance < (edgeTriplet.dstAttr.distance - edgeTriplet.attr)) {
          Iterator((edgeTriplet.dstId, VertexAttribute(
                                          edgeTriplet.dstAttr.cityName,
                                          edgeTriplet.srcAttr.distance + edgeTriplet.attr,
                                          edgeTriplet.srcAttr.path :+ City(edgeTriplet.dstAttr.cityName, edgeTriplet.dstId )
                                       )
                  )
                )
        }
        else {
          Iterator.empty
        }
      },

      // mergeMsg
      (vertexAttr1, vertexAttr2) =>
        if (vertexAttr1.distance < vertexAttr2.distance) vertexAttr1 else vertexAttr2
    )

    for ((destVertexId, attribute) <- shortestPathFunction.vertices) {
      println(s"Going from Arad to ${attribute.cityName} has a distance of ${attribute.distance} km. Path is: ${attribute.path.mkString(" => ")}")
    }

  }

}
