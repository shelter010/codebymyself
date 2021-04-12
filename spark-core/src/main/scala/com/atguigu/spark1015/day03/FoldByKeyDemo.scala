package com.atguigu.spark1015.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/7 下午11:30  
 * @Version 1.0
 */
object FoldByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FoldByKeyDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("hello", "atguigu", "hpf", "hpf", "haha", "hello", "atguigu", "hello"))
    val rdd2 = rdd1.map((_, 1))
    println(rdd2.getNumPartitions)
    val rdd3 = rdd2.foldByKey(1)(_ + _)
    rdd3.collect.foreach(println)

    /**
     * 0值计算了多少次？？
     *   只在分区内（预聚合）时有效
     */
  }
}
