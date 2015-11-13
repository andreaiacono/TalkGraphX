package graphx.builtin

import graphstream.SimpleGraphViewer
import graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.TriangleCount

/**
  * this object contains an example for invoking the Triangle Count algorithm
  * on a graph.
  */
object TriangleCountSample {


  def main(args: Array[String]): Unit = {

    val vertices = USERS_VERTICES_FILENAME
    val edges = USERS_DENSE_EDGES_FILENAME

    // launches the viewer of the graph
    new SimpleGraphViewer(vertices, edges).run();

    // loads a graph with vertices attributes [user, age] and edges not having any attribute
    val sparkContext = getSparkContext()
    val graph = loadGraphFromFiles(sparkContext, vertices, edges)

    run(graph)
  }

  def run(graph: Graph[Person, String]): Unit = {

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
