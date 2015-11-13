package graphx.builtin

import graphstream.SimpleGraphViewer
import graphx._
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

    val vertices = USERS_VERTICES_FILENAME
    val edges = EDGES_FILENAME

    // launches the viewer of the graph
    new SimpleGraphViewer(vertices, edges).run()

    // loads a graph with vertices attributes [user, age] and edges having an attribute
    val sparkContext = getSparkContext()
    val graph = loadGraphFromFiles(sparkContext, vertices, edges)

    run(graph)
  }

  def run(graph: Graph[Person, String]): Unit = {

    // calls the GraphX shortest path algorithm on the graph for nodes 1 and 5 (so we have
    // the shortest path between these two nodes and all the other nodes of the graph)
    val shortestPaths = ShortestPaths.run(graph, List(1, 5)).vertices.collect

    // prints the results
    shortestPaths
      .flatMap {
        case (sourceNode, lengthMap) => lengthMap.map(mapEntry => (sourceNode, mapEntry.id, mapEntry.value))
      }
      .foreach {
        case (sourceNode, destNode, pathLength) => println(s"Shortest path from [$sourceNode] to [$destNode] is $pathLength")
      }
  }


}
