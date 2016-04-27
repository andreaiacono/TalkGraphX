# Talk GraphX
This is the repository for the talk about GraphX given at the [Codemotion Milan](http://milan2015.codemotionworld.com/) conference and at [Voxxed Ticino 2016](https://voxxeddays.com/ticino/). Slides not yet online.

## Requirements

* [SBT](http://www.scala-sbt.org/)
* Java 8

## Usage

    sbt run

And pick the class you want to run.

Every class in package `src/main/scala/graphx` is auto-contained, so you can run it and see how it works by inspecting the code of the class itself.
The package `src/main/scala/graphx/builtin` contains some examples of how to call the GraphX builtin algorithms on some datasets.
A simple graph viewer is automatically launched for every class, so that you can see on the graph the results of computation.

## Data
The datafiles are contained in `src/main/resources/data` directory. The filenames ending in `_edges.txt` are the data files for edges of the graphs, while the files ending in `_vertices.txt` are the ones for the vertices.
Note that for every vertices file there can be more than one edges file.
