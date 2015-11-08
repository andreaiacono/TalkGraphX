package graphx

import misc.{Constants, Utils}

object GraphAggregation {

  def main(args: Array[String]): Unit = {

    val sparkContext = Utils.getSparkContext()

    // loads a graph with edge that do not have any attribtue
    val graph = Utils.loadGraphFromFiles(sparkContext, Constants.USERS_VERTICES_FILENAME, Constants.USERS_EDGES_FILENAME)

    // computes the out degree of every vertex
    val outDegree = graph.aggregateMessages[Int](
            edgeContext => edgeContext.sendToSrc(1), //  sendMsg function : for every edge, we send a 1 to the source of the edge
            (a, b) => a + b                          //  mergeMsg function: every vertex will sum all the received 1s
    ).collect()

    // concise version of the same function call:
//    val outDegree = graph.aggregateMessages[Int](_.sendToSrc(1), _+_) collect

    outDegree.foreach(vertex => println(s"Vertex [${vertex._1}] has ${vertex._2} outgoing edges."))

    // TODO: complete this sample
    val oldestFollower = graph.aggregateMessages[(String, Int)](
      edgeContext => edgeContext.sendToDst(edgeContext.srcAttr), //  sendMsg function : for every edge, we send a 1 to the source of the edge
      (a, b) => if (a._1 > b._1) a else b
    ).collect()

  }
}
