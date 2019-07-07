import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.graphx._

val args = sc.getConf.get("spark.driver.args").split("\\s+")

if (args.length == 0) {
    println("Please pass dataset path as an argument when loading script")
    sys.exit
}

val filepath = args(0)

val toc = System.nanoTime

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, filepath)

val sourceVertex = 1004
val destinationVertex = 3655
val maxRecursionDepth = 1

def has_neighbor( g:Graph[Int, Int], sourceVertex:Long, destinationVertex:Long, maxRecursionDepth:Int ) : Boolean = {

    val neighbors = g.collectNeighborIds(EdgeDirection.Out).lookup(sourceVertex).head
    val result = neighbors contains (destinationVertex)

    // println("Depth: "+ maxRecursion +" Vertex "+ sourceVertex + " Neighbors: ")
    // println(neighbors.mkString("\n"))
    // println("Contains result = "+ result)

    if (result) {
        println("Is reachable")
        return true
    }
    else {
        if (maxRecursionDepth > 0){
            for (i <- 0 until neighbors.length) {
                has_neighbor(g, neighbors(i), destinationVertex, maxRecursionDepth-1)
            }
        }
    }
    return false
}

has_neighbor(graph, sourceVertex, destinationVertex, maxRecursionDepth)

// println("Final result = "+ has_neighbor(graph, sourceVertex, destinationVertex, 1))

val tic = System.nanoTime

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")

// Printing Spark conf properties
println("\n" + sc.getConf.getInt("spark.executor.instances", 123) + "\n")

println(sc.getConf.getAll.mkString("\n") + "\n")

println(sc.getConf.toDebugString + "\n") // Basically same as getAll from above

println(sc.getConf.getExecutorEnv.mkString("\n"))