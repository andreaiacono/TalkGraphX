package graphx.builtin

import misc.{Constants, Utils}
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.TriangleCount

/**
  * this object contains an example for invoking the Triangle Count algorithm
  * on a graph.
  */
object TriangleCountSample {


  def main(args: Array[String]): Unit = {

    val sparkContext = Utils.getSparkContext()
    val graph = Utils.loadGraphFromFiles(sparkContext, Constants.USERS_VERTICES_FILENAME, Constants.USERS_DENSE_EDGES_FILENAME)

    run(graph)
  }

  def run(graph: Graph[(String, Int), String]): Unit = {

    // calls the GraphX triangle count algorithm on the graph
    val triangles = TriangleCount.run(graph)

    // prints how many triangles every vertex participate in
    triangles.vertices
      .foreach {
        case (vertexId, trianglesNumber) => println(s"Vertex [${vertexId}] participates in ${trianglesNumber} triangles.")
      }

    println("Total number of triangles in the graph: " + triangles.vertices
      .map {
        case (vertexId, trianglesNumber) => trianglesNumber.toLong
      }
      .reduce(_ + _ / 3)
    )
  }

}
