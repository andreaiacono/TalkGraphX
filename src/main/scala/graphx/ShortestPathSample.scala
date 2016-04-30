package graphx

import graphstream.SimpleGraphViewer
import graphx.types.{City, VertexAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object ShortestPathSample extends App {

	val vertices = US_CITIES_VERTICES_FILENAME
	val edges = US_CITIES_EDGES_FILENAME

	// launches the viewer of the graph
//	new SimpleGraphViewer(vertices, edges, false).run()

	// loads the graph
	val sparkContext = getSparkContext()
	val graph: Graph[String, Double] = loadCitiesGraphFromFiles(sparkContext, vertices, edges)

	val vprog = (vertexId: VertexId, currentVertexAttr: VertexAttribute, newVertexAttr: VertexAttribute) =>
		if (currentVertexAttr.distance <= newVertexAttr.distance) currentVertexAttr else newVertexAttr

	val sendMsg = (edgeTriplet: EdgeTriplet[VertexAttribute, Double]) => {
		if (edgeTriplet.srcAttr.distance < (edgeTriplet.dstAttr.distance - edgeTriplet.attr)) {
			Iterator(
				(edgeTriplet.dstId,
					new VertexAttribute(
						edgeTriplet.dstAttr.cityName,
						edgeTriplet.srcAttr.distance + edgeTriplet.attr,
						edgeTriplet.srcAttr.path :+ new City(edgeTriplet.dstAttr.cityName, edgeTriplet.dstId)
					)
					)
			)
		}
		else {
			Iterator.empty
		}
	}

	val mergeMsg = (attribute1: VertexAttribute, attribute2: VertexAttribute) =>
		if (attribute1.distance < attribute2.distance) {
			attribute1
		}
		else {
			attribute2
		}

	// launches pregel computation for shortest path
	shortestPath(sparkContext, graph)

	/**
	  * Pregel implementation of Dijkstra algorithm for shortest path:
	  * https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm
	  * @param sparkContext
	  * @param graph
	  */
	def shortestPath(sparkContext: SparkContext, graph: Graph[String, Double]) = {

		// we want to know the shortest paths from the vertex 1 (city of Arad)
		// to all the others vertices (all the other cities)
		val sourceCityId: VertexId = 1L

		// initialize a new graph with data of the old one, plus distance and path;
		// for all vertices except the city we want to compute the path from, the
		// distance is set to infinity
		val initialGraph: Graph[VertexAttribute, Double] = graph.mapVertices(
			(vertexId, cityName) =>
				if (vertexId == sourceCityId) {
					VertexAttribute(
						cityName,
						0.0,
						List[City](new City(cityName, sourceCityId))
					)
				}
				else {
					VertexAttribute(
						cityName,
						Double.PositiveInfinity,
						List[City]())
				}
		)

		// calls pregel
		val shortestPathGraph = initialGraph.pregel(
			initialMsg = VertexAttribute("", Double.PositiveInfinity, List[City]()),
			maxIterations = Int.MaxValue,
			activeDirection = EdgeDirection.Out
		)(
			vprog,
			sendMsg,
			mergeMsg
		)

		// writes the results
		for ((destVertexId, attribute) <- shortestPathGraph.vertices) {
			println(s"Going from Washington to ${attribute.cityName} " +
				s"has a distance of ${attribute.distance} km. " +
				s"Path is: ${attribute.path.mkString(" => ")}")
		}
	}
}