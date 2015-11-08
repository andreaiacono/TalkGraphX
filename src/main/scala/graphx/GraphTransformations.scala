package graphx

import misc.{Constants, Utils}

object GraphTransformations {

  def main(args: Array[String]): Unit = {

    val sparkContext = Utils.getSparkContext()

    // loads a graph with vertices attributes [user, age] and edges not having any attribute
    val graph = Utils.loadGraphFromFiles(sparkContext, Constants.USERS_VERTICES_FILENAME, Constants.USERS_EDGES_FILENAME)

    // prints every triplet of the graph
    graph.triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] has a connection to ${triplet.dstAttr}[${triplet.dstId}]"))

    // creates a subgraph with only the vertices whose name is longer than 3 chars
    graph.subgraph(triplet => triplet.srcAttr._1.length > 3 && triplet.dstAttr._1.length > 3)
      .triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] has a connection to ${triplet.dstAttr}[${triplet.dstId}]"))

    // now adds two attributes (as a tuple) to every edge:
    // - a random value for trust/distrust
    // - a boolean that is true if the length of source username is > 4
    val transformedGraph = graph.mapTriplets(
      triplet =>
        ( if (scala.util.Random.nextInt(2) > 0) "trust" else "distrust",
          triplet.srcAttr._1.size > 4)
    )

    // creates a subgraph with only the "likes" edges
    graph.subgraph(triplet => triplet.attr == "likes")
      .triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} ${triplet.dstAttr}[${triplet.dstId}]"))

    // and prints it
    transformedGraph.triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} to ${triplet.dstAttr}[${triplet.dstId}]"))

    // now add the attribute of username length to every vertex:
    val secondTransformedGraph = transformedGraph.mapVertices(
      (vertexId, attributes) => (vertexId, attributes._1, attributes._2, attributes._1.length)
    )

    // and prints it
    secondTransformedGraph.triplets.foreach(triplet => println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} to ${triplet.dstAttr}[${triplet.dstId}]"))
  }

}
