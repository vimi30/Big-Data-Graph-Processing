import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx._
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



case class Vertex(id:Long,cluster:Long,c:Long, adjacent:List[Long]) 

object Partition {
     def main ( args: Array[String] ) {

          val conf = new SparkConf().setAppName("GraphX")
          val sc = new SparkContext(conf)
          
          var count:Long = 0

          val graph = sc.textFile(args(0)).map {

               line=> {

                    val a= line.split(",").map(_.toLong)
                    count += 1
                    Vertex(a(0), a(0),count, a.tail.toList)
               }
          }

          val edges = graph.flatMap(v =>
                Seq((v.id, v.cluster,v.c)) ++
                v.adjacent.flatMap(adj => Seq((v.id, adj, v.c))) )
               .map(x => {
               new Edge(x._1, x._2, x._3)         
          })



          val graphh = Graph.fromEdges(edges, 1L)
          val clusters = graphh.edges.filter(edge => edge.attr <= 5).map(edge=>{edge.srcId}).distinct().collect()
          val clusterBroadcast = sc.broadcast(clusters)

          val initialGraph = graphh.mapVertices((id, _) => 
          {
            if(clusterBroadcast.value.contains(id))
              id
            else
              -1L
          })

          val finalGraph = initialGraph.pregel(-1L, 5)(
               (id, oldCluster, newCluster) =>{
               if(oldCluster == -1L)
                    newCluster
               else
                    oldCluster
               },
               triplet => {  // Send Message
                         Iterator((triplet.dstId, triplet.srcAttr))
               },
               (a, b) => math.max(a, b) // Merge Message
          )

          val result = finalGraph.vertices.map(v => (v._2, 1)).reduceByKey(_+_).collect().foreach(println)
     }
}