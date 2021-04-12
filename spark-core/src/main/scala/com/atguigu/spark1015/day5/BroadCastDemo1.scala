package com.atguigu.spark1015.day5

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/21 上午9:08  
 * @Version 1.0
 */
object BroadCastDemo1 {
  def main(args: Array[String]): Unit = {
    //广播变量  解决共享变量读的问题
    val conf = new SparkConf().setAppName("BoardCastDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val arr = 1 to 1000 toArray   //假设是一个大大数据数组

    //将大数据的内容广播出去
    val bd = sc.broadcast(arr)
    val list = List(10, 50, 100, 100000, 334243, 1234, 80, 70)

    //将list中包含arr中大内容留下来 并返回
    val rdd1 = sc.parallelize(list,4)
    val rdd2 = rdd1.filter(x => bd.value.contains(x))

    rdd2.collect.foreach(println)
    println(rdd2.getNumPartitions)

    sc.stop()
  }
}
