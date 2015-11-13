package rdd

import graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddSample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDD Sample Application").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = createRdd(sparkContext)
    printRdd(rdd)

    val squaredRdd = rdd.map(x => x*x)
    printRdd(squaredRdd)

    printRdd(loadFromFile(EDGES_FILENAME, sparkContext))
  }

  def createRdd(sparkContext: SparkContext): RDD[Int] = {
    val data = Array(1, 2, 3, 4, 5)
    return sparkContext.parallelize(data)
  }

  def printRdd[A](rdd: RDD[A]): Unit = {
    rdd.foreach(x => print(x + "\n"))
  }

  def loadFromFile(fileName: String, sparkContext: SparkContext): RDD[String] = {
    sparkContext.textFile(fileName)
  }

}

