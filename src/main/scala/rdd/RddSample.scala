package rdd

import org.apache.spark.{SparkConf, SparkContext}

object RddSample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDD Sample Application").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    hardCodedData(sparkContext)
    loadFromFile("/home/andrea/test.txt", sparkContext)
  }

  def loadFromFile(fileName: String, sparkContext: SparkContext): Unit = {
    val dataFile = sparkContext.textFile(fileName, 2).cache()
    return dataFile
  }

  def hardCodedData(sparkContext: SparkContext): Unit = {
    val data = Array(1, 2, 3, 4, 5)
    val numericRDD = sparkContext.parallelize(data)
    val squaredData = numericRDD.map(x => x*x)
    squaredData.foreach(x => print(x + " "))
  }
}

