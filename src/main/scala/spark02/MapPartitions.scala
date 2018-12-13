package spark02

import myspark.MySpark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author tcc
  * @date 2018/12/10 15:42
  * @description
  */
object MapPartitions {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySpark(this.getClass.getSimpleName)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)
    println(rdd.partitions.size)
    val rdd2: RDD[Int] = rdd.mapPartitions(it => {
      println(it)
      it.map(_ * 10)
    })
    val rdd3: RDD[String] = rdd.mapPartitionsWithIndex((i, it) => {
      it.map(t => s"$i,$t")
    })
    rdd3.foreach(println)
    sc.stop()
  }
}
