package com.hpf.sparkstreaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/4/19 下午2:08  
 * @Version 1.0
 */
object TestKafkaUtil {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    //注意：Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    //从kafka消费数据 然后控制台打印
    //1 kafka的参数配置 kafka集群地址 消费者组 key序列化 value序列化,可以
    //可以把这些内容放在map中
    val kafkaParas = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "master:9092,slave1:9092,slave2:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "hpfGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->"org.apache.kafka.common.serialization.StringDeserializer",
      //ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //2 通过kafkaUtil来创建连接
    val kafkaDStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent, //优先位置
      ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaParas) //可订阅多个主题 配置参数
    )

    // 3 将每条消息的kv取出
    val valueDStream = kafkaDStream.map(_.value())

    // 4 计算wordcount
    valueDStream.flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
        .print()


    //启动采集器。 采集器只是一个守护线程，放在print后面可以。
    ssc.start()
    //等待采集结束后终止程序
    ssc.awaitTermination()

  }
}
