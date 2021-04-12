package com.atguigu.spark1015.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/17 下午7:23  
 * @Version 1.0
 */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    // join内连接 左外连接 右外连接 全外连接
    val conf = new SparkConf().setMaster("local[2]").setAppName("JoinDemo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
    val rdd2 = sc.parallelize(Array((1, "aa"), (1, "bb"), (3, "bb"), (2, "cc")))
    //内连接
//    val rdd3 = rdd1.join(rdd2)
    // zuo wai lian jie
val rdd3 = rdd1.leftOuterJoin(rdd2)
    val rdd4 = rdd1.fullOuterJoin(rdd2)
    rdd4.collect().foreach(println)
    sc.stop()
  }
}
