package com.atguigu.spark1015.day03

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/7 下午10:32  
 * @Version 1.0
 */
object PartitionByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("PartitionBy" +
      "Demo")
    val sc = new SparkContext(conf)
    val list1 = List(30, 40, 70, 20, 11, 33)
    //交集
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = rdd1.map((_, 1))
    println(rdd2.partitioner)
    //val rdd3 = rdd2.partitionBy(new HashPartitioner(3))
    //println(rdd3.partitioner)
    //rdd3.collect().foreach(println)

    //如何按照value来分区
    val rdd3 = rdd2.map({
      case (k, v) => (v, k)
    }).partitionBy(new HashPartitioner(5))
      .map({
        case (k, v) => (v, k)
      })   //输出结果都在1分区里面 因为当时partition是根据1 来的 ；1 / 5 的1余数是1
    //rdd3.glom().collect().map(_.toList).foreach(println)
    rdd3.glom().map(_.toList).collect().foreach(println)
    sc.stop()
  }
}
