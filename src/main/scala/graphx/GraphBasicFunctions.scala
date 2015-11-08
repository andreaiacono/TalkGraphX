package graphx

import misc.Utils
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object GraphBasicFunctions {

  def main(args: Array[String]): Unit = {

    val sparkContext = Utils.getSparkContext()

    // create a very simple graph
    val graph = createSampleGraph(sparkContext)

    // prints the number of vertices and edges of the graph
    println(s"The graph has ${graph.numVertices} vertices")
    println(s"The graph has ${graph.numEdges} edges")

    // prints the vertices of the graph
    graph.vertices.foreach(vertex => println(s"VertexId: ${vertex._1} User: ${vertex._2}"))

    // prints the edges of the graph
    graph.edges.foreach(edge => println(s"Edge going from ${edge.srcId} to ${edge.dstId}"))

    // prints the number of incoming edges for every vertex
    graph.inDegrees.foreach(vertex => println(s"VertexId: ${vertex._1} Incoming edges: ${vertex._2}"))

    // prints the number of outgoing edges for every vertex
    graph.outDegrees.foreach(vertex => println(s"VertexId: ${vertex._1} Outgoing edges: ${vertex._2}"))

    // prints the number of all the edges of every vertex (both incoming and outgoing)
    graph.degrees.foreach(vertex => println(s"VertexId: ${vertex._1} Degrees: ${vertex._2}"))

    // prints every triplet of the graph
    graph.triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} ${triplet.dstAttr}[${triplet.dstId}]"))
  }

  /**
    * creates a Graph with hardcoded values of relationships among people
    * @param sparkContext
    * @return the graph
    */
  def createSampleGraph(sparkContext: SparkContext): Graph[String, String] = {

    // vertices RDD
    val users: RDD[(VertexId, String)] =
      sparkContext.parallelize(
        Array(
          (1L, "alice"),
          (2L, "bob"),
          (3L, "carol")
        )
      )

    // edges RDD
    val edges: RDD[Edge[String]] =
      sparkContext.parallelize(
        Array(
          Edge(1L, 2L, "likes"),
          Edge(1L, 3L, "dislikes"),
          Edge(2L, 1L, "likes"),
          Edge(3L, 1L, "dislikes"),
          Edge(3L, 2L, "likes")
        )
      )

    Graph(users, edges)
  }


}
