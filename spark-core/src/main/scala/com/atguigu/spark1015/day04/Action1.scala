package com.atguigu.spark1015.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/17 下午10:36  
 * @Version 1.0
 */
object Action1 {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[2]").setAppName("Action1")
      val sc = new SparkContext(conf)
      val list = List(22, 33, 23, 43, 11, 2, 1)
      //行动算子 collect take count first takeOrdered()()
      val rdd1 = sc.parallelize(list,2)
      val res = rdd1.map(x => {
        println("map....")
        x
      }).filter(x => {
        println("filter...")
        x > 10
      }).takeOrdered(3)(Ordering.Int.reverse).toList
      println(res)
      sc.stop()

  }
}
