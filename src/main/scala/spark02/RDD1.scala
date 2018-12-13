package spark02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author tcc
  * @date 2018/12/10 11:00
  * @description
  */
object RDD1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")//这里表示只分配一个核
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    //读取HDFS数据：
    //默认情况下， 分区数量 =  读取的数据的block块的数量
    //至少是2个,但是还得看分给的核数,如果只有一个,那就只能是1个分区
    //sc.textFile(path,分区的数量),分区数量设置不能小于block块数量,而且至少是2个
    val file: RDD[String] = sc.textFile("hdfs://hadoop102:9000/spark/input.txt")
    println(file.partitions.size)//1


    val arr = Array(1,2,3,4,5)
    //集合并行化中
    //默认情况下， 分区数量 =  application使用的 cores,默认拿可用的所有核
    //sc.makeRDD(data,分区数量)
    val rdd: RDD[Int] = sc.makeRDD(arr)//不指定分区数量
    println(rdd.partitions.size)//1
    val rdd1: RDD[Int] = sc.makeRDD(arr,2)
    println(rdd1.partitions.size)//2

    val rdd2: RDD[Int] = rdd1.map(_*10)
    val arr2 = Array(("zhangfei",1),("guanyu",2))
    val rdd3: RDD[(String, Int)] = sc.makeRDD(arr2)
    val rdd4: RDD[(String, Int)] = rdd3.mapValues(_*10)
    val rddG1: RDD[(Int, Iterable[Int])] = rdd.groupBy(t=>t)

    //groupby,groupbykey,reducebykey
    val arr3  = Array(("bingbing",10),("yangmi",20),("bingbing",20),("yangmi",50))
    val rdd5: RDD[(String, Int)] = sc.makeRDD(arr3,3)
    val rdd6: RDD[(String, Iterable[(String, Int)])] = rdd5.groupBy(_._1)
    val res: RDD[(String, Int)] = rdd6.mapValues(it => {
      val ints: Iterable[Int] = it.map(_._2)
      ints.sum
    })

    //groupByKey按照key分组,并且返回(key,values集合)形式
    val rdd7: RDD[(String, Iterable[Int])] = rdd5.groupByKey()
    val res2: RDD[(String, Int)] = rdd7.mapValues(_.sum)

    //reduceByKey 先已经按照key分完组,然后按照指定的reduce函数进行聚合
    val res3: RDD[(String, Int)] = rdd5.reduceByKey(_+_)
    res3.foreach(println)
    sc.stop()

  }
}
