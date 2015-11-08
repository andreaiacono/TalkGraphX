package misc

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object Utils {


  def getSparkContext(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("RDD Sample Application").setMaster("local")
    new SparkContext(sparkConf)
  }


  def loadEdges(sparkContext: SparkContext, edgesFilename: String): RDD[Edge[String]] = {

    sparkContext.textFile(edgesFilename)
      .map { line =>
        val fields = line.split(" ")
        if (fields.size == 2) {
          Edge(fields(0).toLong, fields(1).toLong)
        }
        else {
          Edge(fields(0).toLong, fields(1).toLong, fields(2))
        }
      }
  }


  def loadVertices(sparkContext: SparkContext, verticesFilename: String): RDD[(VertexId, (String, Int))] = {

    sparkContext.textFile(verticesFilename)
      .filter( line => line.length > 0 && line.charAt(0) != '#')
      .map { line =>
          val fields = line.split(" ")
          (fields(0).toLong, (fields(1), fields(2).toInt))
      }
  }

  /**
    * creates a Graph loading the nodes and the edges from filesystem
    * @param sparkContext
    * @param edgesFilename
    * @param verticesFilename
    * @return
    */
  def loadGraphFromFiles(sparkContext: SparkContext, verticesFilename: String, edgesFilename: String): Graph[(String, Int), String] = {

    val edges = loadEdges(sparkContext, edgesFilename)
    val vertices = loadVertices(sparkContext, verticesFilename)

    Graph(vertices, edges)
  }

  /**
    * prints the relationships among the nodes of the graph
    * @param graph
    */
  def printRelationships(graph: Graph[String, String], separators: (String, String)): Unit = {
    graph.triplets
      .map(triplet => triplet.srcAttr + separators._1 + triplet.attr + separators._2 + triplet.dstAttr + ".")
      .foreach(println(_))
  }

  def printPeopleRelationships(graph: Graph[String, String]): Unit = {
    printRelationships(graph, (" is the ", " of "))
  }
}
