package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/4 下午10:42  
 * @Version 1.0
 */
object SampleDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SampleDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //val list = List(11, 2 2, 33, 44, 55, 66,55,67,767,78,7)
    val list = 1 to 20
    val rdd1 = sc.parallelize(list, 2)
    //按照百分20的概率去抽取  第一个参数表示是否放回抽样 false 表示不放回抽样 【0，1】
    //true 表示放回抽样 比例是【0，无穷大】
    val rdd2 = rdd1.sample(false, 0.2)
    rdd2.collect().foreach(println)

    sc.stop()
  }
}
