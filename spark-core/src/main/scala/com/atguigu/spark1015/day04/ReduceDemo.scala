package com.atguigu.spark1015.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/17 下午11:18  
 * @Version 1.0
 */
object ReduceDemo {
  def main(args: Array[String]): Unit = {
    //reduce 是一个行动算子 直接做哦聚合
    val conf = new SparkConf().setMaster("local[2]").setAppName("Action3")
    val sc = new SparkContext(conf)
    val lis = List(10, 20, 30, 40, 50)
    val rdd1 = sc.parallelize(lis)
    //val res = rdd1.reduce(_+_)
    val res = rdd1.fold(0)(_ + _)
    println(res)

  }
}
