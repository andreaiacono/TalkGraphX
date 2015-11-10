package graphx

import graphstream.SimpleGraphViewer
import misc.{Constants, Utils}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx._


object PregelSample {


  def main(args: Array[String]): Unit = {

    val vertices = Constants.USERS_VERTICES_FILENAME
    val edges = Constants.LIKENESS_EDGES_FILENAME

    // launches the viewer of the graph
    new SimpleGraphViewer(vertices, edges).run()

    // loads the graph
    val sparkContext = Utils.getSparkContext()
    val graph = Utils.loadGraphFromFiles(sparkContext, vertices, edges)
    furthestDistance(sparkContext, graph)
  }

  def furthestDistance(sparkContext: SparkContext, graph: Graph[(String, PartitionID), String]) = {

    Pregel(
      graph.mapVertices((vid, vd) => 0),
      initialMsg = 0,
      activeDirection = EdgeDirection.Out
    )(
      (id, vd, a) => math.max(vd, a),
      et => Iterator((et.dstId, et.srcAttr + 1)),
      math.max(_, _)).vertices.collect.foreach(println(_)
    )
  }

  def sssp(sc: SparkContext) = {

    val graph: Graph[Long, Double] = GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 42 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity, 10, EdgeDirection.Out)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {
        // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    println(sssp.vertices.collect.mkString("\n"))
  }


  def basic_sample(graph: Graph[(String, PartitionID), String]): Unit = {

    val initialMsg = 9999
    def vprog(vertexId: VertexId, value: (String, Int), message: Int): (String, Int) = {
      if (message == initialMsg)
        value
      else
        (value._1, message min value._2)
    }

    def sendMsg(triplet: EdgeTriplet[(String, Int), String]): Iterator[(VertexId, Int)] = {
      val sourceVertex = triplet.srcAttr

      if (sourceVertex._1 == sourceVertex._2)
        Iterator.empty
      else
        Iterator((triplet.dstId, sourceVertex._2))
    }

    def mergeMsg(msg1: Int, msg2: Int): Int = msg1 min msg2

    val minGraph = graph.pregel(initialMsg,
      10,
      EdgeDirection.Out)(
      vprog,
      sendMsg,
      mergeMsg)

    minGraph.vertices.collect.foreach {
      case (vertexId, (value, original_value)) => println(value)
    }
  }

}
