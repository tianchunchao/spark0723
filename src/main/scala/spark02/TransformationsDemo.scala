package spark02

import myspark.MySpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author tcc
  * @date 2018/12/10 19:00
  * @description 转换算子
  */
object TransformationsDemo {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySpark(this.getClass.getSimpleName)
    //1.map  返回值的数据类型，取决于 传递的函数的返回值类型。
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),3)
    val rdd2: RDD[Boolean] = rdd1.map(_>3)
    rdd1.partitions.size//3

    //2.mapValues  作用于 RDD[K,V] ,Key保持不变
    val rdd3: RDD[(String, Int)] = sc.makeRDD(List(("reba",100),("fengjie",50)))
    val rdd4: RDD[(String, Int)] = rdd3.mapValues(_/10)
    rdd4.partitions.size//3

    //3.mapPartitions
    val rdd5: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),3)
    rdd5.mapPartitions(_.map(_*10))//按照分区迭代计算,所以迭代3次,而是像map每个元素都进行一次迭代

    //4.mapPartitionsWithIndex
    val rdd6: RDD[String] = rdd5.mapPartitionsWithIndex((i: Int, it: Iterator[Int]) => {
      it.map(t => s"$i,$t")
    })
//    rdd6.collect().toBuffer.foreach(println)

    //5.flatMap 得到新的rdd的分区数量不变
    val rdd7: RDD[String] = sc.makeRDD(List("hello spark","hello scala"))
    val rdd8: RDD[String] = rdd7.flatMap(_.split(" "))
    rdd8.partitions.size//3

    //6.filter  过滤后分区数量不变，即使某些分区没有数据，但是分区是依然存在的
    val rdd9: RDD[Int] = rdd1.filter(_>3)

    //7.groupBy 返回值类型[(K,Iterable[T])]  K：是传入的函数的返回值类型  T  是rdd的元素类型
    val res1: RDD[(Int, Iterable[Int])] = rdd1.groupBy(t=>t)
    val res2: RDD[(String, Iterable[Int])] = rdd1.groupBy(t=>t.toString)

    //8.groupByKey,RDD[K,V]形式的根据key分组,返回RDD[k,values集合]
    val rdd10: RDD[(String, Int)] = sc.makeRDD(List(("rb", 1000), ("baby", 990),
      ("yangmi", 980), ("bingbing", 5000), ("bingbing", 1000), ("baby", 2000)), 3)
    val rdd11: RDD[(String, Iterable[Int])] = rdd10.groupByKey()

    //9.reduceBykey 先根据key分组,然后values根据函数进行reduce聚合,还可以直接指定生成RDD的分区数量
    val rdd12: RDD[(String, Int)] = rdd10.reduceByKey(_+_,2)

    //10.sortby 倒序的两种写法
    rdd10.sortBy(_._2,false)
    rdd10.sortBy(-_._2)

    //11.sortByKey
    rdd10.sortByKey(false)//根据key倒序

    //12.union并集,intersection交集,subtract差集
    val rdd13: RDD[Int] = sc.makeRDD(List(1,2,9),2)
    val rdd14: RDD[Int] = rdd13.union(rdd1)//生成的分区数为两者分区数相加
    val rdd15: RDD[Int] = rdd1.intersection(rdd13)//生成的RDD分区取较大的
    val rdd16: RDD[Int] = rdd1.subtract(rdd13)//生成的RDD分区根据左边的这个取

    //13.distinct 集合元素去重,默认分区数量不变,可以传参改变,底层是调用reduceByKey
    rdd1.distinct()

    //14.拉链操作
    val rdd17: RDD[(Int, Boolean)] = rdd1.zip(rdd2)
    val rdd18: RDD[(Int, Long)] = rdd1.zipWithIndex()
    rdd16.foreach(println)
    println(rdd16.partitions.size,rdd15.partitions.size,rdd14.partitions.size)
    sc.stop()
  }
}
