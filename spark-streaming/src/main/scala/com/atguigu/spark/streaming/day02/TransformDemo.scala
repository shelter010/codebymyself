package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/2/28 下午11:54  
 * @Version 1.0
 */
object TransformDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("abc")
    val ssc = new StreamingContext(conf, Seconds(3))

    //用到transform算子 可以将流转换为rdd来进行计算 其实流的底层也是rdd  先有数据源
    val sourceStream = ssc.socketTextStream("master", 9999)
    val res = sourceStream.transform(rdd => {
      rdd.flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey(_ + _)
    })


    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
