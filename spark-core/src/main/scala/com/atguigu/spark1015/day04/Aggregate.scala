package com.atguigu.spark1015.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/17 下午11:28  
 * @Version 1.0
 */
object Aggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Action4")
    val sc = new SparkContext(conf)
    val lis = List(10, 20, 30, 40, 50)
    val rdd1 = sc.parallelize(lis)
    val res = rdd1.aggregate(1)(_ + _, _ + _)
    println(res)
    sc.stop()
  }
}
