package graphx

import graphstream.SimpleGraphViewer
import misc.{Constants, Utils}

object GraphAggregation {

  def main(args: Array[String]): Unit = {

    val vertices = Constants.USERS_VERTICES_FILENAME
    val edges = Constants.LIKENESS_EDGES_FILENAME

    // launches the viewer of the graph
    new SimpleGraphViewer(vertices, edges).run()

    // loads the graph
    val sparkContext = Utils.getSparkContext()
    val graph = Utils.loadGraphFromFiles(sparkContext, vertices, edges)

    // computes the out degree of every vertex
    // concise version of the same function call: graph.aggregateMessages[Int](_.sendToSrc(1), _+_)
    graph
      .aggregateMessages[Int](
        edgeContext => edgeContext.sendToSrc(1), //  sendMsg function : for every edge, we send a 1 to the source of the edge
        (a, b) => a + b //  mergeMsg function: every vertex will sum all the received 1s
      )
      .foreach(
        vertex => println(s"Vertex [${vertex._1}] has ${vertex._2} outgoing edges.")
      )

    // compute the oldest incoming user of every vertex
    graph
      .aggregateMessages[Int](
        edgeContext => edgeContext.sendToDst(edgeContext.srcAttr._2), // sendMsg function : for every edge, we send the age of the source to the destination
        (a, b) => if (a > b) a else b // mergeMsg function: we compare the received ages and choose the higher
      )
      .foreach(
        vertex => println(s"The oldest incoming user for vertex [${vertex._1}] is ${vertex._2} years old.")
      )

    // compute the oldest follower younger than 40 of every vertex
    graph
      .aggregateMessages[Int](
        // sendMsg function : for every edge, we send the age of the source to the destination only if
        // the source is younger than 35 and is "following" the destination
        edgeContext => if (edgeContext.srcAttr._2 < 40 && edgeContext.attr == "follows") {
                            edgeContext.sendToDst(edgeContext.srcAttr._2)
                       },
        // mergeMsg function: we compare the received ages and choose the higher
        (a, b) => if (a > b) a else b
      )
      .foreach(
        vertex => println(s"The oldest follower younger than 40 for vertex [${vertex._1}] is ${vertex._2} years old.")
      )
  }
}
