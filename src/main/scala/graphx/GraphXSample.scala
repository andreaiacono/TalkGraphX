package graphx

import misc.Constants
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object GraphXSample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDD Sample Application").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val graph = loadGraphFromFile(sparkContext, Constants.EDGES_FILENAME, Constants.VERTICES_FILENAME)

    // uncomment this line to create a hardcoded graph
//    val graph = createSampleGraph(sparkContext)

    printPeopleRelationships(graph)
  }

  /**
    * creates a Graph with hardcoded values of relationships among people
    * @param sparkContext
    * @return the graph
    */
  def createSampleGraph(sparkContext: SparkContext): Graph[String, String] = {

    // vertices RDD
    val users: RDD[(VertexId, String)] =
      sparkContext.parallelize(
        Array(
          (1L, "andrea"),
          (2L, "chiara"),
          (3L, "bianca"),
          (4L, "federico"),
          (5L, "donatella"),
          (6L, "giulia"),
          (7L, "tommaso"),
          (8L, "giorgia")
        )
      )

    // edges RDD
    val edges: RDD[Edge[String]] =
      sparkContext.parallelize(
        Array(
          Edge(1L, 2L, "husband"),
          Edge(1L, 3L, "father"),
          Edge(1L, 4L, "brother-in-law"),
          Edge(1L, 5L, "brother-in-law"),
          Edge(1L, 6L, "uncle"),
          Edge(1L, 7L, "uncle"),
          Edge(1L, 8L, "friend"),
          Edge(2L, 1L, "wife"),
          Edge(2L, 3L, "mother"),
          Edge(2L, 4L, "sister"),
          Edge(2L, 5L, "sister-in-law"),
          Edge(2L, 6L, "aunt"),
          Edge(2L, 7L, "aunt"),
          Edge(2L, 8L, "friend"),
          Edge(3L, 1L, "daughter"),
          Edge(3L, 2L, "daughter"),
          Edge(3L, 4L, "niece"),
          Edge(3L, 5L, "niece"),
          Edge(3L, 6L, "cousin"),
          Edge(3L, 7L, "cousin"),
          Edge(4L, 1L, "brother-in-law"),
          Edge(4L, 2L, "brother"),
          Edge(4L, 3L, "uncler"),
          Edge(4L, 5L, "husband"),
          Edge(4L, 6L, "father"),
          Edge(4L, 7L, "father"),
          Edge(4L, 8L, "friend"),
          Edge(5L, 1L, "sister-in-law"),
          Edge(5L, 2L, "sister-in-law"),
          Edge(5L, 3L, "aunt"),
          Edge(5L, 4L, "wife"),
          Edge(5L, 6L, "mother"),
          Edge(5L, 7L, "mother"),
          Edge(6L, 1L, "niece"),
          Edge(6L, 2L, "niece"),
          Edge(6L, 3L, "cousin"),
          Edge(6L, 4L, "daughter"),
          Edge(6L, 5L, "daughter"),
          Edge(6L, 7L, "sister"),
          Edge(7L, 1L, "nephew"),
          Edge(7L, 2L, "nephew"),
          Edge(7L, 3L, "cousin"),
          Edge(7L, 4L, "son"),
          Edge(7L, 5L, "son"),
          Edge(7L, 6L, "brother"),
          Edge(8L, 1L, "friend"),
          Edge(8L, 2L, "friend"),
          Edge(8L, 3L, "friend"),
          Edge(8L, 4L, "friend"))
      )

    Graph(users, edges)
  }

  def loadGraphFromFile(sparkContext: SparkContext, edgesFileName: String, verticesFileName: String): Graph[String, String] = {

    val edges: RDD[Edge[String]] =
      sparkContext.textFile(edgesFileName)
        .map { line =>
                  val fields = line.split(" ")
                  Edge(fields(0).toLong, fields(1).toLong, fields(2))
        }
    val people: RDD[(VertexId, String)] =
      sparkContext.textFile(verticesFileName)
        .map { line =>
          val fields = line.split(",")
          (fields(0).toLong, fields(1))
        }

    Graph(people, edges)
  }

  /**
    * prints the relationships among the nodes of the graph
    * @param graph
    */
  def printPeopleRelationships(graph: Graph[String, String]): Unit = {
    graph.triplets
      .map(triplet => triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr + ".")
      .foreach(println(_))
  }
}
