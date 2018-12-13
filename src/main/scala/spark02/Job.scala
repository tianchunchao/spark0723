package spark02

import myspark.MySpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
  * @author tcc
  * @date 2018/12/10 22:07
  * @description
  */
object Job {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySpark(this.getClass.getSimpleName)
    val list: List[String] = Source.fromFile("movie.txt").getLines().toList
    val rdd: RDD[String] = sc.makeRDD(list)
    val rdd2: RDD[Array[(String, String, String, String)]] = rdd.map(r => {
      val str: Array[String] = r.split(",")
      val id: String = str(0)
      val ty: String = str(1)
      val show: String = str(2)
      val click: String = str(3)
      val types: Array[String] = ty.split("\\|")
      types.map(t => {
        (id, t, show, click)
      })
    })
    val rdd4: RDD[((String, String), Int, Int)] = rdd2.flatMap(f=>{
        f.map(t => {
          val id: String = t._1
          val typ: String = t._2
          val show: Int = t._3.toInt
          val click: Int = t._4.toInt
          ((id, typ), show, click)
      })
    })
   /* val rdd4: RDD[((String, String), Int, Int)] = rdd3.map(t => {
      val id: String = t._1
      val typ: String = t._2
      var show: Int = t._3.toInt
      val click: Int = t._4.toInt
      ((id, typ), show, click)
    })*/
    val rdd5: RDD[((String, String), (Int, Int))] = rdd4.groupBy(_._1).mapValues(t => {
      val sum1: Int = t.map(_._2).sum
      val sum2: Int = t.map(_._3).sum
      (sum1, sum2)
    })
    val rdd6: RDD[(String, String, Int, Int)] = rdd5.map(a=>(a._1._1,a._1._2,a._2._1,a._2._2))
    val res: RDD[(String, String, Int, Int)] = rdd6.sortBy(_._1)
    res.foreach(println)

    sc.stop()
  }
}
