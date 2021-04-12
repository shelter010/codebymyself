package com.atguigu.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/2/28 下午6:18
 * @Version 1.0
 */
object WordCountKafkaDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("a").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    //通过直连的方式从kafka消费数据
    val params = Map[String, String](
      "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
      "group.id" -> "1015"
    )

    val sourceStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      params,
      Set("first1015")
    )

    //对数据流做操作
    sourceStream.flatMap {
      case (_, v) =>
        v.split("\\W+")
    }.map((_, 1))
      .reduceByKey(_ + _)
      .print()


    ssc.start()
    ssc.awaitTermination()
  }
}
