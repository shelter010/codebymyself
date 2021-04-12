package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/4 下午9:44  
 * @Version 1.0
 */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("FlatMapDemo")
    val sc = new SparkContext(conf)
//    val list = List(1 to 4, 10 to 15, 20 to 24)
//    val rdd1 = sc.parallelize(list)
//    //列表里的列表输出
//    val rdd2 = rdd1.flatMap(x => x)
    //rdd中存储列表元素中的2次方和3次方
    val list2 = List(11, 23, 34, 56, 13)
//    val rdd3 = sc.parallelize(list2, 2)
//    val rdd4 = rdd3.flatMap(x => List(x, x * x, x * x * x))
//    rdd4.collect().foreach(println)

    // 将其中偶数的2次方和三次方输出
    val rdd5 = sc.parallelize(list2, 3)
    val rdd6 = rdd5.flatMap(x => if (x % 2 == 0) List(x, x * x) else List())
     rdd6.collect().foreach(println)
    //    rdd2.collect().foreach(println)

    sc.stop()

  }
}
