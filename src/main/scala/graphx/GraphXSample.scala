package graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object GraphXSample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDD Sample Application").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val graph = createSampleGraph(sparkContext)
    printGraph(graph)
  }

  def createSampleGraph(sparkContext: SparkContext): Graph[(String, String), String] = {

    // vertices' RDD
    val users: RDD[(VertexId, (String, String))] =
      sparkContext.parallelize(
        Array(
          (1L, ("andrea", "Disbrain")),
          (2L, ("luca", "BitGold")),
          (3L, ("pierre", "disbrain")),
          (4L, ("gian carlo", "JRC")))
      )

    // edges' RDD
    val edges: RDD[Edge[String]] =
      sparkContext.parallelize(
        Array(
          Edge(1L, 2L, "friend"),
          Edge(1L, 3L, "colleague"),
          Edge(1L, 4L, "former colleague"),
          Edge(4L, 2L, "friend"))
      )

    val defaultUser = ("John Doe", "Missing")
    Graph(users, edges, defaultUser)
  }

  def printGraph(graph: Graph[(String, String), String] ): Unit = {
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " (" + triplet.srcAttr._2 + ") is a " + triplet.attr + " of " + triplet.dstAttr._1 + " (" + triplet.dstAttr._2 + ").")
    facts.collect.foreach(println(_))
  }
}
