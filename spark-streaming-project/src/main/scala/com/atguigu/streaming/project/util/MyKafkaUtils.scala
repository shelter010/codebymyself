package com.atguigu.streaming.project.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
 * @author hpf
 * @create 2021/3/2 上午7:46  
 * @Version 1.0
 */
object MyKafkaUtils {
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "bigdata1015",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  /**
   * 根据传入的参数 返回从kafka得到的流
   * @param ssc
   * @param topics
   * @return
   */
  def getKafkaStream(ssc: StreamingContext, topics: String*) =
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, //标配
      Subscribe[String, String](topics, kafkaParams)
    ).map(_.value())
}



