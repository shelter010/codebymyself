package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/2/28 下午12:18  
 * @Version 1.0
 */
object wordCount1 {
  def main(args: Array[String]): Unit = {
    //1 创建 streaming context
    val conf= new SparkConf().setAppName("wordCount1").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    //2 从个数据源创建1个流 socket rdd队列 自定义接收器 kafka 重点
    val sourceStream = ssc.socketTextStream("master", 9999)
    // 对流做各种转换
    val res = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //4 行动算子 print foreach foreachRdd
    res.print()   //把结果打印在控制台
    //5 启动流
    ssc.start()
    //6 阻止主线程退出
    ssc.awaitTermination()
  }
}
