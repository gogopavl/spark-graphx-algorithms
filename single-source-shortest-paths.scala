import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths

val args = sc.getConf.get("spark.driver.args").split("\\s+")

if (args.length == 0) {
    println("Please pass dataset path as an argument when loading script")
    sys.exit
}

val filepath = args(0)

val toc = System.nanoTime

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, filepath)

// Source and destination
val sourceVertex = 1004
val destinationVertex = 1007

// Find the shortest paths
val result = ShortestPaths.run(graph, Seq(destinationVertex)) // Result is a graph

// Get vertices RDD, filter to get sp from sourceVertex and get shortest path to destinationVertex - Option object
val shortestPath = result.vertices.filter({case(vId, _) => vId == sourceVertex}).first._2.get(destinationVertex).map(_.toString).getOrElse("")

println("\nShortest path from vertex "+ sourceVertex +" to vertex "+ destinationVertex +": "+ shortestPath)

val tic = System.nanoTime

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")

// Printing Spark conf properties
println("\n" + sc.getConf.getInt("spark.executor.instances", 123) + "\n")

println(sc.getConf.getAll.mkString("\n") + "\n")

println(sc.getConf.toDebugString + "\n") // Basically same as getAll from above

println(sc.getConf.getExecutorEnv.mkString("\n"))
