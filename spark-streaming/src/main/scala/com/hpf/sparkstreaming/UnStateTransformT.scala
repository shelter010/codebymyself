package com.hpf.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/4/19 下午10:22  
 * @Version 1.0
 */
object UnStateTransformT {
  def main(args: Array[String]): Unit = {
    //从socket读取 然后使用transfrom算子
    //创建配置文件对象
    //注意：Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val inputDStream = ssc.socketTextStream("master", 7777)
    //转换为算子操作
    val resDS = inputDStream.transform(
      rdd => {
        rdd.flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _)
          .sortByKey()
      }
    )
    resDS.print()


    //启动采集器。 采集器只是一个守护线程，放在print后面可以。
    ssc.start()
    //等待采集结束后终止程序
    ssc.awaitTermination()

  }
}
