import org.apache.spark.graphx.GraphLoader

val args = sc.getConf.get("spark.driver.args").split("\\s+")

if (args.length == 0) {
    println("Please pass dataset path as an argument when loading script")
    sys.exit
}

val filepath = args(0)

val toc = System.nanoTime

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, filepath)

// Run PageRank
val ranks = graph.staticPageRank(20).vertices

val tic = System.nanoTime

ranks.take(10) // So that the calculation takes place

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")

// Print top 10 pages based on their pagerank
ranks.sortBy(_._2, ascending=false).take(10).foreach(println)

