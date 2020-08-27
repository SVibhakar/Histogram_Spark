import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Histogram {
    /* type Color = (int,int) */
    
    def main ( args: Array[ String ] ) {
    /* ... */
    val conf = new SparkConf().setAppName("Histogram")
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                            (((1, a(0)), 1), ((2, a(1)), 1), ((3, a(2)), 1)) } )
    val r = input.map(s => s._1)
    val g = input.map(s => s._2)
    val b = input.map(s => s._3)
    
    val red = r.reduceByKey((s,t) => (s+t))
    val green = g.reduceByKey((s,t) => (s+t))
    val blue = b.reduceByKey((s,t) => (s+t))
    
    /*val color = List(r,g,b)
    var res = color.sum*/
    val color = r.union(g).union(b)
    val answer = color.map{ case ((x,y),z) => x + "\t" + y + "\t" + z }
    answer.collect().foreach(println)
    sc.stop()
  }
}

/*val r = input.map(s => s._1).reduceByKey(_+_)
    val g = input.map(s => s._2).reduceByKey(_+_)
    val b = input.map(s => s._3).reduceByKey(_+_)
    */