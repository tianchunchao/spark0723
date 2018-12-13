package spark02

import myspark.MySpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author tcc
  * @date 2018/12/10 20:09
  * @description Action算子
  */
object ActionsDemo {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySpark(this.getClass.getSimpleName)
    //1.foreach
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),3)
    rdd1.foreach(println)
    //2.foreachPartition,每次迭代一个分区的数量,类似mapPartition
    rdd1.foreachPartition(println)//这样遍历出三个迭代器
    rdd1.foreachPartition(it=>println(it.mkString("-")))//1   2-3   4-5 三行数据
    //3.reduce 归并:得到的结果数据顺序是不确定的,因为数据分布在不同的executor,但是本地模式模拟不出来,因为只有一台电脑
    val rdd2: RDD[String] = sc.makeRDD(List("a","b","c"))
    val rdd3: String = rdd2.reduce(_+_)

    //4.count,求集合元素个数
    val count: Long = rdd1.count()
    //5.first,求集合第一个元素
    val first: Int = rdd1.first()
    //6.take,取集合前几个
    val take: Array[Int] = rdd1.take(3)
    //7.top:倒序排序并取集合中的前几个
    val top: Array[Int] = rdd1.top(4)
    //8.takeOrdered:升序排序,并取前几个
    val takeOrdered: Array[Int] = rdd1.takeOrdered(2)
    //9.countByKey:根据key求个数,返回值为map类型
    val rdd4: RDD[(String, Long)] = rdd2.zipWithIndex()
    val countByKey: collection.Map[String, Long] = rdd4.countByKey()

    //10.collect
    val rdd5: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2)))
    val collect: Array[(String, Int)] = rdd5.collect()
    //11.collectAsMap,只能作用于RDD[K,V]
    val sollectAsMap: collection.Map[String, Int] = rdd5.collectAsMap()
//    println(rdd3)
    sc.stop()
  }
}
