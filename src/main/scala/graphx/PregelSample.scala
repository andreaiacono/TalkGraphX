package graphx

import graphstream.SimpleGraphViewer
import misc.{Constants, Utils}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._


object PregelSample {

  def main(args: Array[String]): Unit = {

    val vertices = Constants.CITIES_VERTICES_FILENAME
    val edges = Constants.CITIES_EDGES_FILENAME

    // launches the viewer of the graph
    new SimpleGraphViewer(vertices, edges, false).run()

    // loads the graph
    val sparkContext = Utils.getSparkContext()
    val graph = Utils.loadCitiesGraphFromFiles(sparkContext, vertices, edges)

    // launches pregel computation
    shortestPath(sparkContext, graph)
  }


  def shortestPath(sc: SparkContext, graph: Graph[String, Double]) = {

    // we want to know the shortest paths from this vertex to all the others vertices
    val vertexSourceId: VertexId = 1L  // vertexId 1 is the city of Arad

    // initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph : Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) => if (id == vertexSourceId) (0.0, List[VertexId](vertexSourceId)) else (Double.PositiveInfinity, List[VertexId]()))

    val shortestPaths = initialGraph.pregel(
      (Double.PositiveInfinity, List[VertexId]()),  // initial message
      Int.MaxValue,                                 // max iterations
      EdgeDirection.Out                             // where to send messages
    )(
      // vertex program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

      // sendMsg
      triplet => {
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },
      // mergeMsg
      (a, b) => if (a._1 < b._1) a else b
    )

    shortestPaths.vertices.collect.foreach {
       case (destVertexId, (distance, path)) => println(s"Going from Vertex 1 to $destVertexId has a distance of $distance km. Path is ${path.mkString(", ")}")
    }
  }

}
