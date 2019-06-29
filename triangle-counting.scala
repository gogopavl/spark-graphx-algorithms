import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

val args = sc.getConf.get("spark.driver.args").split("\\s+")

if (args.length == 0) {
    println("Please pass dataset path as an argument when loading script")
    sys.exit
}

val filepath = args(0)

val toc = System.nanoTime

// Load the edges in canonical order and partition the graph for triangle count
val graph = GraphLoader.edgeListFile(sc, filepath, true)
  .partitionBy(PartitionStrategy.RandomVertexCut)

// Find the triangle count for each vertex
val triCounts = graph.triangleCount().vertices.collect().map(_._2).sum

val tic = System.nanoTime

println("Triangle counts: "+ triCounts/3)

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")

// Printing Spark conf properties
println("\n" + sc.getConf.getInt("spark.executor.instances", 123) + "\n")

println(sc.getConf.getAll.mkString("\n") + "\n")

println(sc.getConf.toDebugString + "\n") // Basically same as getAll from above

println(sc.getConf.getExecutorEnv.mkString("\n"))
