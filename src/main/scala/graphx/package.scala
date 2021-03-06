import graphx.types.Person
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

package object graphx {

  type CityName = String
  type Distance = Double

  implicit class VertexIdTuple[A](t: (VertexId, A)) {
    val id: VertexId = t._1
    val value: A = t._2
  }

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


  def loadVertices(sparkContext: SparkContext, verticesFilename: String): RDD[(VertexId, Person)] = {

    sparkContext.textFile(verticesFilename)
      .filter( line => line.length > 0 && line.charAt(0) != '#')
      .map { line =>
          val fields = line.split(" ")
          (fields(0).toLong, Person(fields(1), fields(2).toInt))
      }
  }

  /**
    * creates a Graph of people loading the nodes and the edges from filesystem
    * @param sparkContext
    * @param edgesFilename
    * @param verticesFilename
    * @return
    */
  def loadPersonFromFiles(sparkContext: SparkContext, verticesFilename: String, edgesFilename: String): Graph[Person, String] = {

    val edges = loadEdges(sparkContext, edgesFilename)
    val vertices = loadVertices(sparkContext, verticesFilename)

    Graph(vertices, edges)
  }

  /**
    * creates a Graph of cities loading the nodes and the edges from filesystem
    * @param sparkContext
    * @param edgesFilename
    * @param verticesFilename
    * @return
    */
  def loadCitiesGraphFromFiles(sparkContext: SparkContext, verticesFilename: String, edgesFilename: String): Graph[CityName, Distance] = {

    val edges = sparkContext.textFile(edgesFilename)
      .map { line =>
              val fields = line.split(" ")
              Edge(fields(0).toLong, fields(1).toLong, fields(2).toDouble)
      }

    val vertices = sparkContext.textFile(verticesFilename)
      .filter( line => line.length > 0 && line.charAt(0) != '#')
      .map { line =>
              val fields = line.split(" ")
              (fields(0).toLong, (fields(1)))
      }

    Graph(vertices, edges)
  }
  /**
    * creates a Graph of values loading the nodes and the edges from filesystem
    * @param sparkContext
    * @param edgesFilename
    * @param verticesFilename
    * @return
    */
  def loadValuesGraphFromFiles(sparkContext: SparkContext, verticesFilename: String, edgesFilename: String): Graph[Int, Int] = {

    val edges = sparkContext.textFile(edgesFilename)
      .map { line =>
              val fields = line.split(" ")
              Edge(fields(0).toLong, fields(1).toLong, 0)
      }

    val vertices = sparkContext.textFile(verticesFilename)
      .filter( line => line.length > 0 && line.charAt(0) != '#')
      .map { line =>
              val fields = line.split(" ")
              (fields(0).toLong, (fields(1).toInt))
      }

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

  def sum(a: Int, b: Int) = a + b

  def pickTheOlderOne(a: Int, b: Int) = a max b

  // CONSTANTS
  val EDGES_FILENAME = "src/main/resources/data/relationships_edges.txt"
  val CITIES_EDGES_FILENAME = "src/main/resources/data/cities_edges.txt"
  val CITIES_VERTICES_FILENAME = "src/main/resources/data/cities_vertices.txt"
  val US_CITIES_EDGES_FILENAME = "src/main/resources/data/us_cities_edges.txt"
  val US_CITIES_VERTICES_FILENAME = "src/main/resources/data/us_cities_vertices.txt"
  val MAXVALUE_EDGES_FILENAME = "src/main/resources/data/maxvalue_edges.txt"
  val MAXVALUE_VERTICES_FILENAME = "src/main/resources/data/maxvalue_vertices.txt"
  val LIKENESS_EDGES_FILENAME = "src/main/resources/data/likeness_edges.txt"
  val PAPERS_EDGES_FILENAME = "src/main/resources/data/papers_edges.txt"
  val USERS_EDGES_FILENAME = "src/main/resources/data/users_edges.txt"
  val USERS_DISJOINT_EDGES_FILENAME = "src/main/resources/data/users_disjoint_edges.txt"
  val USERS_DENSE_EDGES_FILENAME = "src/main/resources/data/users_dense_edges.txt"
  val USERS_VERTICES_FILENAME = "src/main/resources/data/users_vertices.txt"
  val VERTICES_FILENAME = "src/main/resources/data/people_vertices.txt"
  val CSS_FILENAME = "src/main/resources/css/style.css"

}
