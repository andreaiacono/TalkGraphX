package graphx

import graphstream.SimpleGraphViewer
import org.apache.spark.graphx.{VertexRDD, Graph}

object GraphAggregation {

  def main(args: Array[String]): Unit = {

    val vertices = USERS_VERTICES_FILENAME
    val edges = LIKENESS_EDGES_FILENAME

    // launches the viewer of the graph
    new SimpleGraphViewer(vertices, edges).run()

    // loads the graph
    val sparkContext = getSparkContext()
    val graph: Graph[Person, String] = loadGraphFromFiles(sparkContext, vertices, edges)

    // computes the out degree of every vertex
    val outDegreeOfEveryVertex: VertexRDD[Int] =
      graph.aggregateMessages[Int](
        sendMsg = edgeContext => edgeContext.sendToSrc(1), // for every edge, we send a 1 to the source of the edge
        mergeMsg = sum                                     // every vertex will sum all the received 1s
      )

    for (outDegree <- outDegreeOfEveryVertex) {
       println(s"Vertex [${outDegree._1}] has ${outDegree._2} outgoing edges.")
    }

    // compute the oldest incoming user of every vertex
    val oldestIncomingUserOfEveryVertex =
      graph.aggregateMessages[Int](
        // for every edge, we send the age of the source to the destination
        sendMsg = edgeContext => edgeContext.sendToDst(edgeContext.srcAttr.age),
        // we compare the received ages and choose the higher
        mergeMsg = pickTheOlderOne
      )

    for (oldestUser <- oldestIncomingUserOfEveryVertex) {
      println(s"The oldest incoming user for vertex [${oldestUser.id}] is ${oldestUser.value} years old.")
    }

    // compute the oldest follower younger than 40 of every vertex
    val oldestFollowerYoungerThan40 =
      graph.aggregateMessages[Int](
        // for every edge, we send the age of the source to the destination only if
        // the source is younger than 35 and is "following" the destination
        sendMsg = edgeContext =>
          if (edgeContext.srcAttr.age < 40 && edgeContext.attr == "follows") {
            edgeContext.sendToDst(edgeContext.srcAttr.age)
          },
        // we compare the received ages and choose the higher
        mergeMsg = pickTheOlderOne
      )

    for (oldestUser <- oldestFollowerYoungerThan40) {
      println(s"The oldest follower younger than 40 for vertex [${oldestUser.id}] is ${oldestUser.value} years old.")
    }

  }

}
