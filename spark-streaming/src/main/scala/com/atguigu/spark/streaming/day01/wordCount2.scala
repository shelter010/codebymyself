package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author hpf
 * @create 2021/2/28 下午12:32  
 * @Version 1.0
 */
object wordCount2 {
  def main(args: Array[String]): Unit = {
    val conf1 = new SparkConf().setAppName("wo").setMaster("local[2]")
    val ssc1 = new StreamingContext(conf1, Seconds(2))

    val socketStreaming = ssc1.socketTextStream("master", 9898)

    //对流做各种转换
    val res = socketStreaming.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

     res.print()

    //启动流
    ssc1.start()
    //阻止流断开
    ssc1.awaitTermination()
    print(13579)
  }
}
