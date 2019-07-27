import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.graphx._

val args = sc.getConf.get("spark.driver.args").split("\\s+")

if (args.length == 0) {
    println("Please pass dataset path, source and target vertices, and max recursion depth as arguments when loading script")
    sys.exit
}

val filepath = args(0)
val sourceVertex = args(1).toLong
val destinationVertex = args(2).toLong
val maxRecursionDepth = args(3).toInt

val toc = System.nanoTime

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, filepath)

def hasNeighbor( g:Graph[Int, Int], sourceVertex:Long, destinationVertex:Long, maxRecursionDepth:Int ) : Boolean = {

    val neighbors = g.collectNeighborIds(EdgeDirection.Out).lookup(sourceVertex).head
    val result = neighbors contains (destinationVertex)

    if (result) {
        return true
    }
    else {
        if (maxRecursionDepth > 1){
            for (i <- 0 until neighbors.length) {
                if (hasNeighbor(g, neighbors(i), destinationVertex, maxRecursionDepth-1)) {
                    return true
                }
            }
        }
    }
    return false
}

println("Vertex "+ destinationVertex +" is reachable from vertex "+ sourceVertex +" = "+ hasNeighbor(graph, sourceVertex, destinationVertex, maxRecursionDepth))

val tic = System.nanoTime

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")