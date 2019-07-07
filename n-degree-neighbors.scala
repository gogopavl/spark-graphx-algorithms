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

// Source vertex
val sourceVertex = 1004

// Neighborhood degree
val degree = 2

def nthDegreeNeighbors( g:Graph[Int, Int], sourceVertex:Long, degree:Int ) : Unit = {

    val neighbors = g.collectNeighborIds(EdgeDirection.Out).lookup(sourceVertex).head

    // println("Depth: "+ maxRecursion +" Vertex "+ sourceVertex + " Neighbors: ")
    // println(neighbors.mkString("\n"))
    // println("Contains result = "+ result)

    if (degree == 1) {
        println(neighbors.mkString(" ")) // Should add them to a data structure instead
        return
    }
    else {
        for (i <- 0 until neighbors.length) {
                nthDegreeNeighbors(g, neighbors(i), degree-1)
        }
    }
}

nthDegreeNeighbors(graph, sourceVertex, degree)

// println("Final result = "+ has_neighbor(graph, sourceVertex, destinationVertex, 1))

val tic = System.nanoTime

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")

// Printing Spark conf properties
println("\n" + sc.getConf.getInt("spark.executor.instances", 123) + "\n")

println(sc.getConf.getAll.mkString("\n") + "\n")

println(sc.getConf.toDebugString + "\n") // Basically same as getAll from above

println(sc.getConf.getExecutorEnv.mkString("\n"))
