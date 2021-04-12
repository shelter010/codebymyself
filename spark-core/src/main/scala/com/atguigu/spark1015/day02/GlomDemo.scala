package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author hpf
 * @create 2021/2/4 下午10:14  
 * @Version 1.0
 */
object GlomDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("GlomDemo")
    val sc = new SparkContext(conf)
    //Filter 将符合条件的留下 有boolean的判断值
    val list = List(30, 50, 70, 60, 10, 20)
    //
    val rdd1 = sc.parallelize(list, 3)
    //glom 将每个分区的元素 放到集合中 最后所有的集合放到1个数组中
    val rdd2 = rdd1.glom().map(x => x.toList)
    rdd2.collect().foreach(println)

    sc.stop()
  }
}
