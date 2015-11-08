package graphx.builtin

import misc.{Constants, Utils}
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths

/**
  * this object contains an example for invoking the Shortest Path algorithm
  * on a graph.
  * The algorithm computes shortest paths to the given set of
  * landmark vertices, returning a graph where each vertex attribute is a map
  * containing the shortest-path distance to each reachable landmark.
  */
object ShortestPathSample {


  def main(args: Array[String]): Unit = {

    val sparkContext = Utils.getSparkContext()
    val graph = Utils.loadGraphFromFiles(sparkContext, Constants.VERTICES_FILENAME, Constants.LIKENESS_EDGES_FILENAME)

    run(graph)
  }

  def run(graph: Graph[(String, Int), String]): Unit = {

    // calls the GraphX shortest path algorithm on the graph for nodes 1 and 5 (so we have
    // the shortest path between these two nodes and all the other nodes of the graph)
    val shortestPaths = ShortestPaths.run(graph, List(1, 5)).vertices.collect

    // prints the results
    shortestPaths
      .flatMap {
        case (sourceNode, lengthMap) => lengthMap.map(mapEntry => (sourceNode, mapEntry._1, mapEntry._2))
      }
      .foreach {
        case (sourceNode, destNode, pathLength) => println(s"Shortest path from [$sourceNode] to [$destNode] is $pathLength")
      }
  }


}
