package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/4 上午8:00  
 * @Version 1.0
 */
object PartitionWithDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PartitionDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = Array(11, 22, 33, 44, 55, 66, 77)
    val rdd1 = sc.parallelize(list, 3)
    val rdd2 = rdd1.mapPartitionsWithIndex((index, it) => {
      it.map(x => (index, x))
    })
    rdd2.collect.foreach(println)

    sc.stop()

  }
}
