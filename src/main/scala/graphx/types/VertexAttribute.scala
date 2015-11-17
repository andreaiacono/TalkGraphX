package graphx.types

import graphx.City

case class VertexAttribute(cityName: String, distance: Double, path: List[City])