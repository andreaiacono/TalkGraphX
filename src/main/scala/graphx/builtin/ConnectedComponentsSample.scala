package graphx.builtin

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

    val sparkContext = Utils.getSparkContext()
    val graph = Utils.loadGraphFromFiles(sparkContext, Constants.USERS_VERTICES_FILENAME, Constants.USERS_DISJOINT_EDGES_FILENAME)

    run(graph)
  }

  def run(graph: Graph[(String, Int), String]): Unit = {

    // calls the GraphX connected components algorithm on the graph
    val connectedComponents = ConnectedComponents.run(graph)

    connectedComponents.vertices
      .foreach {
        case (vertexId: Long, setId: Long) => println(s"Vertex [${vertexId}] belongs to Set [${setId}]")
      }
  }


}
