package com.atguigu.spark1015.day01

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/4/15 下午10:24  
 * @Version 1.0
 */
object lTest444 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.parallelize(List(1,2,3,4),2)
    println(rdd.aggregate(10)(_ + _, _ + _))

    //4.关闭连接
    sc.stop()

  }
}
