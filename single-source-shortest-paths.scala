import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths

val args = sc.getConf.get("spark.driver.args").split("\\s+")

if (args.length == 0) {
    println("Please pass dataset path and source vertex as arguments when loading script")
    sys.exit
}

val filepath = args(0)
val sourceVertex = args(1).toLong
val destinationVertex = args(2).toLong

val toc = System.nanoTime

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, filepath)

// Find the shortest paths
val result = ShortestPaths.run(graph, Seq(destinationVertex)) // Result is a graph

// Get vertices RDD, filter to get sp from sourceVertex and get shortest path to destinationVertex - Option object
val shortestPath = result.vertices.filter({case(vId, _) => vId == sourceVertex}).first._2.get(destinationVertex).map(_.toString).getOrElse("")

println("\nShortest path from vertex "+ sourceVertex +" to vertex "+ destinationVertex +": "+ shortestPath)

val tic = System.nanoTime

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")