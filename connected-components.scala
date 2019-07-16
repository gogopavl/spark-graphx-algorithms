import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

val args = sc.getConf.get("spark.driver.args").split("\\s+")

if (args.length == 0) {
    println("Please pass dataset path as an argument when loading script")
    sys.exit
}

val filepath = args(0)

val toc = System.nanoTime

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, filepath)

// Find the connected components
val connComp = graph.connectedComponents().vertices.collect()

// .mkString("\n")
// println(connComp)

val tic = System.nanoTime

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")