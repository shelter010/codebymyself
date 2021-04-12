package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/6 下午6:11  
 * @Version 1.0
 */
object Double1Demo {
  def main(args: Array[String]): Unit = {
    //交集 并 差 笛卡尔积
    val conf = new SparkConf().setMaster("local[2]").setAppName("DoubleDemo")
    val sc = new SparkContext(conf)
    val list1 = List(30, 40, 70, 20, 11, 33, 66, 88)
    val list2 = List(30, 4, 70, 2, 1, 3, 6, 88)
    //交集
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = sc.parallelize(list2, 3)
    //val rdd3 = rdd1.intersection(rdd2)

    //并集
    //val rdd3 = rdd1.union(rdd2)
   // val rdd3 = rdd1 ++ rdd2

    //差集
    //val rdd3 = rdd1.subtract(rdd2)

    //笛卡尔积 输出结果是 一段段的元组
    val rdd3 = rdd1.cartesian(rdd2)
    rdd3.collect().foreach(println)
    sc.stop()
  }

}
