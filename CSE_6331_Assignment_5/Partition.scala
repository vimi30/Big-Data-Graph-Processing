import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer

object Partition {

  val depth = 6
  

  def main ( args: Array[ String ] ) {

    val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)
    var count = 0;

    var graph = sc.textFile(args(0)).map( line => { val a = line.split(",")

    var id = a(0).toLong

    var cluster: Long = -1

    var list = ListBuffer[Long]()

    if(count<5){
      cluster = id
      count += 1;
    }
    

    for(i <- 1 to a.length-1)
    {
      list += a(i).toLong
    }

    (id,cluster,list.toList)

    })

 
    for (i <- 1 to depth){
      graph = graph.flatMap{ case(id, cluster, adjacent) => (id,cluster) ; adjacent.map(point => (point,cluster))}
                    .reduceByKey(_ max _)
                    .join( graph.map( line => (line._1,line) ))
                    .map(line =>
                    if(line._2._2._2>0)
                    {
                      (line._2._2._1,line._2._2._2,line._2._2._3)
                    }else
                    {
                      (line._2._2._1,line._2._1,line._2._2._3)
                    }
                    )

                    }
      
    graph.map( line => (line._2,1)).reduceByKey(_ + _).collect.foreach(println)
    //graph.collect.foreach(println)

 

  }
}
