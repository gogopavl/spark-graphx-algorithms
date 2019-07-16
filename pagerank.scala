import org.apache.spark.graphx.GraphLoader

// Get arguments string array
val args = sc.getConf.get("spark.driver.args").split("\\s+")

if (args.length == 0) {
    println("Please pass dataset path as an argument when loading script")
    sys.exit
}

val filePath = args(0)

val toc = System.nanoTime

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, filePath)

// Run PageRank

val ranks = graph.staticPageRank(20).vertices

ranks.take(10) // So that the calculation takes place

val tic = System.nanoTime

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")