package graphx

import graphstream.SimpleGraphViewer
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

    // step #1: define the initial message, max iterations and active direction
    val shortestPathFunction = initialGraph.pregel(
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
          Iterator( (edgeTriplet.dstId, new VertexAttribute(
                                              edgeTriplet.dstAttr.cityName,
                                              edgeTriplet.srcAttr.distance + edgeTriplet.attr,
                                              edgeTriplet.srcAttr.path :+ new City(edgeTriplet.dstAttr.cityName, edgeTriplet.dstId )))
          )
        }
        else {
          Iterator.empty
        }
      },

      // mergeMsg
      (city1, city2) => if (city1.distance < city2.distance) city1 else city2

    )

    for ((destVertexId, attribute) <- shortestPathFunction.vertices) {
      println(s"Going from Arad to ${attribute.cityName} has a distance of ${attribute.distance} km. Path is: ${attribute.path.mkString(" => ")}")
    }

  }

}
