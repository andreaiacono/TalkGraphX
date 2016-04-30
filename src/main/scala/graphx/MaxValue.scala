package graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object MaxValue extends App {

	val vertices = MAXVALUE_VERTICES_FILENAME
	val edges = MAXVALUE_EDGES_FILENAME

	// loads the graph
	val sparkContext = getSparkContext()
	val graph: Graph[Int, Int] = loadValuesGraphFromFiles(sparkContext, vertices, edges)

	println("Graph initial state")
	graph.vertices.map(v => "Node [" + v._1 + "]: " + v._2).foreach(println)

	// launches pregel computation for max value
	val maxGraph = maxValue(sparkContext, graph)

	println("\nGraph final state")
	maxGraph.vertices.map(v => "Node [" + v._1 + "]: " + v._2).foreach(println)

	println(s"\nMax value of the graph is ${maxGraph.vertices.first()._2}.")

	def maxValue(sparkContext: SparkContext, graph: Graph[Int, Int]): Graph[Int, Int] = {

		graph.pregel(
			initialMsg = Int.MinValue,
			maxIterations = Int.MaxValue,
			activeDirection = EdgeDirection.Out
		)(
			// vprog
			(vertexId: VertexId, currentVertexAttr: Int, newVertexAttr: Int) =>
				if (newVertexAttr > currentVertexAttr) newVertexAttr else currentVertexAttr,

			// sendMsg
			(edgeTriplet: EdgeTriplet[Int, Int]) => {
				if (edgeTriplet.srcAttr > edgeTriplet.dstAttr)
					Iterator( (edgeTriplet.dstId, edgeTriplet.srcAttr) )
				else
					Iterator.empty
			},

			// mergeMsg
			(attribute1: Int, attribute2: Int) => if (attribute1 > attribute2) attribute1 else attribute2
		)
	}
}