package com.atguigu.spark1015.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/7 下午10:54  
 * @Version 1.0
 */
object ReduceByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReduceByDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("hello", "atguigu", "hpf", "hpf", "haha", "hello", "atguigu", "hello"))
    val rdd2 = rdd1.map((_, 1))
    val rdd3 = rdd2.reduceByKey(_ + _)
    rdd3.collect().foreach(println)
    sc.stop()
  }
}
