package graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphBasicFunctions extends App {

    val sparkContext = getSparkContext()

    // create a very simple graph
    val graph = createSampleGraph(sparkContext)

    // prints the number of vertices and edges of the graph
    println(s"The graph has ${graph.numVertices} vertices")
    println(s"The graph has ${graph.numEdges} edges")

    // prints the vertices of the graph
    for (vertex <- graph.vertices) {
      println(s"VertexId: ${vertex.id} User: ${vertex.value}")
    }

    // prints the edges of the graph
    for (edge <- graph.edges) {
      println(s"Edge going from ${edge.srcId} to ${edge.dstId}")
    }

    for (vertex <- graph.inDegrees) {
      println(s"VertexId: ${vertex.id} Incoming edges: ${vertex.value}")
    }

    for (vertex <- graph.outDegrees) {
      println(s"VertexId: ${vertex.id} Outgoing edges: ${vertex.value}")
    }

    for (vertex <- graph.degrees) {
      println(s"VertexId: ${vertex.id} Degrees: ${vertex.value}")
    }

    // prints every triplet of the graph
    for (triplet <- graph.triplets) {
      println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} ${triplet.dstAttr}[${triplet.dstId}]")
    }

    // prints the neighbours of every vertex as Arrays
    for (vertex <- graph.collectNeighbors(EdgeDirection.Either)) {
      println(s"Vertex [${vertex.id}] has neighbours: [${vertex.value.distinct.mkString(" ")}]")
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
