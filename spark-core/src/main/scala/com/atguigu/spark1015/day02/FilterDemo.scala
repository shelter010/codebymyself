package com.atguigu.spark1015.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/4 下午10:02  
 * @Version 1.0
 */
object FilterDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("FilterDemo")
    val sc = new SparkContext(conf)
    //Filter 将符合条件的留下 有boolean的判断值
    val list = List(30, 50, 70, 60, 10, 20)
    val rdd1: RDD[Int] = sc.parallelize(list, 2)
    val rdd2: RDD[Int] = rdd1.filter(x => x > 50)
    rdd2.collect().foreach(println)

    sc.stop()
  }


}
