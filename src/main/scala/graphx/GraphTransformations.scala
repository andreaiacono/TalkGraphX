package graphx

import graphstream.SimpleGraphViewer
import misc.{Constants, Utils}

object GraphTransformations {

  def main(args: Array[String]): Unit = {

    val vertices = Constants.USERS_VERTICES_FILENAME
    val edges = Constants.USERS_EDGES_FILENAME

    // launches the viewer of the graph
    new SimpleGraphViewer(vertices, edges).run();

    // loads a graph with vertices attributes [user, age] and edges not having any attribute
    val sparkContext = Utils.getSparkContext()
    val graph = Utils.loadGraphFromFiles(sparkContext, vertices, edges)

    // prints every triplet of the graph
    println("Triplets:")
    graph.triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] has a connection to ${triplet.dstAttr}[${triplet.dstId}]"))

    // prints a subgraph with only the vertices whose name is longer than 3 chars
    println("Subgraph with names longer than 3:")
    graph.subgraph(triplet => triplet.srcAttr._1.length > 3 && triplet.dstAttr._1.length > 3)
      .triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] has a connection to ${triplet.dstAttr}[${triplet.dstId}]"))

    // now adds two attributes (as a tuple) to every edge:
    // - a random value for trust/distrust
    // - a boolean that is true if the length of source username is > 4
    val transformedGraph = graph.mapTriplets(
      triplet =>
        ( if (scala.util.Random.nextInt(2) > 0) "trusts" else "distrusts",
          triplet.srcAttr._1.size > 4)
    )
    println("Added attributes to edge:")
    transformedGraph.triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} ${triplet.dstAttr}[${triplet.dstId}]"))

    // prints a subgraph with only the "trusts" edges
    println("Subgraph with only 'trust' attribute:")
    transformedGraph.subgraph(triplet => triplet.attr._1 == "trusts")
      .triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} ${triplet.dstAttr}[${triplet.dstId}]"))

    // now add the attribute of username length to every vertex:
    val secondTransformedGraph = transformedGraph.mapVertices(
      (vertexId, attributes) => (vertexId, attributes._1, attributes._2, attributes._1.length)
    )
    println("Now added an attribute to the vertices:")
    secondTransformedGraph.triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} to ${triplet.dstAttr}[${triplet.dstId}]"))
  }

}
