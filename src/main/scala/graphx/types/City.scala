package graphx.types

import org.apache.spark.graphx.VertexId

case class City(name: String, id: VertexId) {
  override def toString() = name + " [" + id + "]"
}
