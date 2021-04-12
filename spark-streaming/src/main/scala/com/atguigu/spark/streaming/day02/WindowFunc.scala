package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/3/1 下午7:34  
 * @Version 1.0
 */
object WindowFunc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("aaa").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceStream = ssc.socketTextStream("master", 9999)
    //滑动窗口 👆的3 是每3秒钟batch一次 计算一次 滑动窗口是倍数
    ssc.checkpoint("ck1")
    sourceStream.flatMap(_.split("\\W+"))
      .map((_, 1))
      //每6s 按照倍数来
    //  .reduceByKeyAndWindow(_+_,Seconds(6))
      //每6s 计算一次最近9s的wordcount
    //  .reduceByKeyAndWindow(_+_,Seconds(9),slideDuration = Seconds(6))
      .reduceByKeyAndWindow(_+_,_-_,Seconds(9),slideDuration = Seconds(6)
      ,filterFunc = _._2>0)
      .print()

    ssc.start()
    ssc.awaitTermination()


  }
}
