import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  type Point = (Double,Double)

  var centroids: Array[Point] = Array[Point]()

  def main(args: Array[ String ]) {
    /* ... */
    val conf = new SparkConf().setAppName("K-Mean")
    val sc = new SparkContext(conf)

    centroids = sc.textFile(args(1)).map( line => { val a = line.split(","); (a(0).toDouble, a(1).toDouble) }).collect;

    val points = sc.textFile(args(0)).map( line => {val b = line.split(",") ; (b(0).toDouble, b(1).toDouble)} )

    for ( i <- 1 to 5 ){

      val cs = sc.broadcast(centroids);

      centroids = points.map { p => (cs.value.minBy(distance(p,_)), p)}.groupByKey().map { case(_ , cpoints) =>
                         
                         var count: Int = 0
                         var xsum: Double = 0.0
                         var ysum: Double = 0.0

                         for(point <- cpoints )
                         {
                           count += 1
                           xsum += point._1
                           ysum += point._2
                         }
                         (xsum/count,ysum/count)
                         }.collect
   
      }

    centroids.foreach(println)

    //poitns.foreach(println)
  }

  def distance(a:(Double,Double), b : (Double,Double)) : Double ={

    var distnc:Double = Math.sqrt((b._2 - a._2)*(b._2 - a._2) + (b._1 - a._1)*(b._1 - a._1));

    return distnc;
  }
}
