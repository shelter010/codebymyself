package com.atguigu.spark1015.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/17 下午11:04  
 * @Version 1.0
 */
object CountByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Action2")
    val sc = new SparkContext(conf)
    val list = List("atguigu", "hpf", "hello", "atguigu", "hpf", "haha", "hpf")
    //做wordcount 基本的做法
    val rdd1 = sc.parallelize(list)
    //    val res = rdd1.map((_, 1)).reduceByKey(_ + _).collect()
    //    res.foreach(println)

    // use countByKey 实现上述的功能
//    val res = rdd1.map((_, 1)).countByKey()
//    val res1 = res.get(key = "hpf")
//    val res2 = res.getOrElse(key = "dada", 1)
//    println(res1) //输出的结果直接是map
//    println(res2)
val wordCount = rdd1.map((_, null)).countByKey()
    println(wordCount)
    sc.stop()
  }
}
