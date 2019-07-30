import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.graphx._
import scala.collection.mutable._

val args = sc.getConf.get("spark.driver.args").split("\\s+")

if (args.length == 0) {
    println("Please pass dataset path, source vertex, and neighorhood degree as arguments when loading script")
    sys.exit
}

val filepath = args(0)
val sourceVertex = args(1).toLong
val degree = args(2).toInt

val neighborSet = scala.collection.mutable.Set[Long]()

val toc = System.nanoTime

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, filepath)

def nthDegreeNeighbors( g:Graph[Int, Int], sourceVertex:Long, degree:Int ) : Unit = {

    val neighbors = g.collectNeighborIds(EdgeDirection.Out).lookup(sourceVertex).head

    if (degree == 1) {
        for(vertex <- neighbors) {
            neighborSet.add(vertex.toLong) // Add vertex ids to global set
        }
        return
    }
    else {
        for (i <- 0 until neighbors.length) {
            nthDegreeNeighbors(g, neighbors(i), degree-1)
        }
    }
    return
}

nthDegreeNeighbors(graph, sourceVertex, degree)

println("Vertex's "+ sourceVertex +" neighbors of degree "+ degree +" are: "+ neighborSet)

val tic = System.nanoTime

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")