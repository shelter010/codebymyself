package com.hpf.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/4/19 上午11:31  
 * @Version 1.0
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    //注意：Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    //读取数据
    val inputDStream = ssc.socketTextStream("master", 7777)
    val redDStream = inputDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    redDStream.print()

    //启动采集器。 采集器只是一个守护线程，放在print后面可以。
    ssc.start()
    //等待采集结束后终止程序
    ssc.awaitTermination()

  }
}
