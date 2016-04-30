package graphx

import graphstream.SimpleGraphViewer
import util.Random

object GraphTransformations extends App {

  val vertices = USERS_VERTICES_FILENAME
  val edges = USERS_EDGES_FILENAME

  // launches the viewer of the graph
  new SimpleGraphViewer(vertices, edges).run();

  // loads a graph with vertices attributes [user, age] and edges not having any attribute
  val sparkContext = getSparkContext()
  val graph = loadPersonFromFiles(sparkContext, vertices, edges)

  // prints every triplet of the graph
  println("Triplets:")

  for (triplet <- graph.triplets) {
    println(s"${triplet.srcAttr}[${triplet.srcId}] has a connection to ${triplet.dstAttr}[${triplet.dstId}]")
  }

  // prints a subgraph with only the vertices whose name is longer than 3 chars
  println("Subgraph with names longer than 3:")
  val subgraphWithNamesLongerThan3 =
    graph.subgraph(triplet => triplet.srcAttr.name.length > 3 && triplet.dstAttr.name.length > 3)

  for (triplet <- subgraphWithNamesLongerThan3.triplets) {
    println(s"${triplet.srcAttr}[${triplet.srcId}] has a connection to ${triplet.dstAttr}[${triplet.dstId}]")
  }

  // now add two attributes (as a tuple) to every edge:
  val graphWithTrustAndLength = graph.mapTriplets(
    triplet =>
      (
        if (Random.nextBoolean) "trusts" else "distrusts", // attr #1: a random value for trust/distrust
        triplet.srcAttr.name.size > 4                      // attr #2: is true if source username length > 4
      )
  )
  println("Added attributes to edge:")

  for (triplet <- graphWithTrustAndLength.triplets) {
    println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} ${triplet.dstAttr}[${triplet.dstId}]")
  }

  // prints a subgraph with only the "trusts" edges
  println("Subgraph with only 'trust' attribute:")
  val subgraphWithTrustedEdges =
    graphWithTrustAndLength.subgraph(triplet => triplet.attr._1 == "trusts")

  for (triplet <- subgraphWithTrustedEdges.triplets) {
    println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} ${triplet.dstAttr}[${triplet.dstId}]")
  }

  // now add the attribute of username length to every vertex:
  val graphWithUsernameLength = graphWithTrustAndLength.mapVertices(
    (vertexId, attributes) =>
      (vertexId, attributes.name, attributes.age, attributes.name.length)
  )

  for (triplet <- graphWithUsernameLength.triplets) {
    println(s"${triplet.srcAttr}[${triplet.srcId}] ${triplet.attr} to ${triplet.dstAttr}[${triplet.dstId}]")
  }

}
