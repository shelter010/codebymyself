package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/3 下午10:47  
 * @Version 1.0
 */
object MapRddDemo {
  def main(args: Array[String]): Unit = {
    //将一个数组 里面的每个数字x2输出
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapRddDemo")
    val sc = new SparkContext(conf)
    val arr = Array(11, 22, 33, 44, 55, 66, 100)
    //从scala集合获得 rdd
    val rdd = sc.parallelize(arr, 2)
    //转换
    val rdd1 = rdd.map(_ * 2)
    //行动算子
    val result = rdd1.collect()
    //print yi xia
    result.foreach(println)

    //最后一定要关闭 SparkContext
    sc.stop()
  }
}
