package graphx.builtin

import graphstream.SimpleGraphViewer
import graphx._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{VertexRDD, GraphLoader}
import org.apache.spark.graphx.lib.PageRank

/**
  * this object contains an example for invoking the PageRank algorithm
  * on a graph.
  * There are two ways to call the PageRank algorithm:
  * - object oriented: using the method PageRank of the Graph class (got from GraphOps class)
  * - object based: using the run() method of the PageRank object
  */
object PageRankSample {

  def main(args: Array[String]): Unit = {

    val vertices = USERS_VERTICES_FILENAME
    val edges = PAPERS_EDGES_FILENAME

    // launches the viewer of the graph
    new SimpleGraphViewer(vertices, edges).run();

    // loads a graph with vertices attributes [user, age] and edges not having any attribute
    val sparkContext = getSparkContext()

    // two ways to call the same algorithm
    runObjectBased(sparkContext, vertices, edges)
    runObjectOriented(sparkContext, vertices, edges)
  }

  def runObjectOriented(sparkContext: SparkContext, verticesFilename: String, edgesFilename: String): Unit = {

    // call the GraphX PageRank algorithm from the graph, passing the tolerance
    val graph = GraphLoader.edgeListFile(sparkContext, edgesFilename)
    val ranks = graph.pageRank(0.001).vertices

    printResults(ranks, sparkContext, verticesFilename)
  }

  def runObjectBased(sparkContext: SparkContext, verticesFilename: String, edgesFilename: String): Unit = {

    // call the GraphX PageRank algorithm passing the graph, stopping at 5 iterations
    val graph = GraphLoader.edgeListFile(sparkContext, edgesFilename)
    val ranks = PageRank.run(graph, 5).vertices

    printResults(ranks, sparkContext, verticesFilename)
  }

  def printResults(ranks: VertexRDD[Double], sparkContext: SparkContext, verticesFilename: String): Unit = {

    // prints the pagerank for every vertex
    println("Ranks: \n" + ranks
      .map {
        case (vertexId, rank) => "VertexId " + vertexId + " has rank " + rank
      }
      .collect().mkString("\n"))

    // since we want to have the ranks related to usernames instead of vertexIds,
    // we join the ranks with the user info (the vertices file) and then print
    val users = loadVertices(sparkContext, verticesFilename)
    val ranksByUsername = users
      .join(ranks)
      .map {
        case (id, (username, rank)) => "User " + username + " [" + id + "] has rank " + rank
      }

    println("Ranks: \n" + ranksByUsername.collect().mkString("\n"))
  }

}
