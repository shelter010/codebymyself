package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author hpf
 * @create 2021/2/28 下午12:49  
 * @Version 1.0
 */
object WordCount3 {
  def main(args: Array[String]): Unit = {
    //从rdd队列中读取数列
    val conf = new SparkConf().setMaster("local[2]").setAppName("ha")
    val ssc = new StreamingContext(conf, Seconds(3))

    //建1个rdd队列
    val rdds = mutable.Queue[RDD[Int]]()

    val socketStream = ssc.queueStream(rdds,false)
    val res1 = socketStream.reduce(_ + _)
    res1.print()

    ssc.start()
    val sc = ssc.sparkContext
    while (true) {
     rdds.enqueue( (sc.parallelize(1 to 100)))
      Thread.sleep(100)
    }

    ssc.awaitTermination()
  }
}
