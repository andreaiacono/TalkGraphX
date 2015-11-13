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
    val vertexSourceId: VertexId = 1L

    // initialize the graph such that all vertices except the root have distance infinity
    val initialGraph: Graph[(Double, List[VertexId]), Double] =
      graph.mapVertices((id, _) =>
        if (id == vertexSourceId)
          (0.0, List[VertexId](vertexSourceId))
        else
          (Double.PositiveInfinity, List[VertexId]()))

    // step #1: define the initial message, max iterations and active direction
    val shortestPathFunction = initialGraph.pregel(
      initialMsg = (Double.PositiveInfinity, List[VertexId]()),
      maxIterations = Int.MaxValue,
      activeDirection = EdgeDirection.Out) _

    // step #2: define the vertex program, send message and merge message
    val result = shortestPathFunction(

      // vprog
      (vertexId, currentAttr, newAttr) =>
        if (currentAttr.distance < newAttr.distance) currentAttr else newAttr,

      // sendMsg
      edgeTriplet => {
        if (edgeTriplet.srcAttr.distance < (edgeTriplet.dstAttr.distance - edgeTriplet.attr)) {
          Iterator((edgeTriplet.dstId, (edgeTriplet.srcAttr.distance + edgeTriplet.attr, edgeTriplet.srcAttr.vertices :+ edgeTriplet.dstId)))
        } else {
          Iterator.empty
        }
      },

      // mergeMsg
      (city1, city2) => if (city1.distance < city2.distance) city1 else city2

    )

    for ((destVertexId, (distance, path)) <- result.vertices) {
      println(s"Going from Vertex 1 to $destVertexId has a distance of $distance km. Path is ${path.mkString(", ")}")
    }

  }

}
