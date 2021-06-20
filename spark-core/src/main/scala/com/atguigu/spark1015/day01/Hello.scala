package com.atguigu.spark1015.day01

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/3 上午7:35  
 * @Version 1.0
 */
object Hello {
  def main(args: Array[String]): Unit = {
    //1 创建1个SparkContext 打包时候 把master设置去掉 在提交时候用 --master来设置master
    //val conf = new SparkConf().setMaster("local[2]").setAppName("Hello")
    val conf = new SparkConf().setAppName("Hello")
    val sc = new SparkContext(conf)

    //2 从数据源得到1个RDD
    val lineRdd = sc.textFile(args(0))

    //3 对RDD进行各种转换
    val resultRdd = lineRdd
      .flatMap(_.split("\\W+"))
      .map((_, 1))
      .reduceByKey(_ + _)
    //4 执行1个行动算子
    val wordCountArr = resultRdd.collect()
    wordCountArr.foreach(println)
    //5 关闭SparkContext
    sc.stop()
  }
}
