package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/4 下午10:19  
 * @Version 1.0
 */
object GroupByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val list = List(11, 22, 33, 44, 55, 66)

    //用group by 进行分组
    val rdd1 = sc.parallelize(list, 2)
    val rdd2 = rdd1.groupBy(x => x % 2)
    //rdd2.collect().foreach(println)  //存的结果是元组
    //求每个分组里面的和 因为groupBy之后存的是元只组 因为是元祖 一进一出 flatMap是
    //一进多出   下面的偏函数适合处理元组
    val rdd3 = rdd2.map {
      case (k, it) => (k, it.sum)
    }
    rdd3.collect().foreach(println)
    sc.stop()
  }

}
