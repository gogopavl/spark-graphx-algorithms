import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.graphx._

val args = sc.getConf.get("spark.driver.args").split("\\s+")

if (args.length == 0) {
    println("Please pass dataset path, source and target vertices as arguments when loading script")
    sys.exit
}

val filepath = args(0)
val sourceVertex = args(1).toLong
val destinationVertex = args(2).toLong

val toc = System.nanoTime

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, filepath)

// Initialize the graph such that all vertices except the root have distance infinity
val initialGraph = graph.mapVertices((id, _) =>
    if (id == sourceVertex) 0.0 else Double.PositiveInfinity)

val sssp = initialGraph.pregel(Double.PositiveInfinity)(
  (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
  triplet => {  // Send Message
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  (a, b) => math.min(a, b) // Merge Message
)

println("Vertex "+ sourceVertex +" and "+ destinationVertex +": "+ sssp.vertices.filter {case (id, _) => id == destinationVertex }.collect.mkString("\n"))

val tic = System.nanoTime

println("Total runtime: "+ (tic-toc)/1e9d + " seconds")