package com.atguigu.spark1015.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/7 下午11:09  
 * @Version 1.0
 */
object GroupByKeyDemo {
  /**
   * 1 分组 按照key进行分组
   * groupBy(x=>...) 按照返回值来分组
   * groupByKey只能用kv形式的
   * groupBy任意rdd都可以使用
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupByKeyDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("hello", "atguigu", "hpf", "hpf", "haha", "hello", "atguigu", "hello"))
    val rdd2 = rdd1.map((_, 1))
    val rdd3 = rdd2.groupByKey()
    //如何求出wordcount呢
        //.mapValues((_2 => _2.sum))
    //或者下面这个也可以的
        .mapValues(_.sum)
    rdd3.collect().foreach(println)
    sc.stop()
  }
}
