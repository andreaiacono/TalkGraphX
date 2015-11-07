# Talk GraphX
This is the repository for the talk about GraphX given at the [Codemotion Milan](http://milan2015.codemotionworld.com/) conference in november 2015.

## Compile and run
To compile and run, you have to create a scala project and add as libraries for the project the jar

    spark-assembly-1.5.1-hadoop2.6.0.jar

that you can find in the `lib` directory of Apache Spark v1.5.1 tarball.
To download the tarball, you have to go to [download page](http://spark.apache.org/downloads.html) of Apache Spark project and choose:

* Spark release: 1.5.1
* Package type: Pre-built for Hadoop 2.6 and later
* Download type: the one you prefer  :-)

You also need the GraphStream v1.3 jars:

* [gs-ui-1.3.zip](http://graphstream-project.org/media/data/gs-ui-1.3.zip)
* [gs-core-1.3.zip](http://graphstream-project.org/media/data/gs-core-1.3.zip)

Spark 1.5.1 uses Scala version 2.10, so you need to use a compatible Scala version (2.10.x).

Every class in package `src/main/scala/graphx` is auto-contained, so you can run it and see how it works by inspecting the code of the class itself.

## Data
The datafiles are contained in `src/main/resources/data` directory. The filenames ending in `_edges.txt` are the data files for edges of the graphs, while the files ending in `_vertices.txt` are the ones for the vertices.
Note that for every vertices file there can be more than one edges file.

## Contents
The package `src/main/scala/graphx/builtin` contains some examples of how to call the GraphX builtin algorithms on some datasets.
There's a simple graph viewer in `src/main/java/graphstream/SimpleGraphViewer`: you can choose the dataset to view changing the file specified in `addDataFromFile()` method.

