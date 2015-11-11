package graphx.builtin

import graphstream.SimpleGraphViewer
import misc.{Constants, Utils}
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ConnectedComponents

/**
  * this object contains an example for invoking the Connected Components.
  * The algorithm finds out if the graph is connected, assigning to each
  * vertex a number: all the vertices that have the same number, are connected.
  */
object ConnectedComponentsSample {


  def main(args: Array[String]): Unit = {

    val vertices = Constants.USERS_VERTICES_FILENAME
    val edges = Constants.USERS_DISJOINT_EDGES_FILENAME

    // launches the viewer of the graph
    new SimpleGraphViewer(vertices, edges, false).run();

    // loads a graph with vertices attributes [user, age] and edges not having any attribute
    val sparkContext = Utils.getSparkContext()
    val graph = Utils.loadGraphFromFiles(sparkContext, vertices, edges)

    run(graph)
  }

  def run(graph: Graph[(String, Int), String]): Unit = {

    // calls the GraphX connected components algorithm on the graph
    val connectedComponents = ConnectedComponents.run(graph)

    connectedComponents.vertices.groupBy( { case (vertexId: Long, setId: Long) => setId } )
      .foreach {
        case (setId: Long, verticesIds) =>
          println(s"Set [${setId}] contains [${verticesIds.map { case (setNumber, vertexId) => setNumber } mkString(", ")}]")
      }
  }


}
