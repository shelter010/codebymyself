package com.atguigu.spark.streaming.day01.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/2/28 下午8:24
 * @Version 1.0
 */
object WordCount666 {

  val groupId = "1015"

  val params = Map[String, String](
    "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
    "group.id" -> groupId
  )
  // kafkaUtils kafkaCluster
  val cluster = new KafkaCluster(params)

  val topics = Set("first1015")

  def readOffsets() = {
    var resultMap = Map[TopicAndPartition, Long]()
    //获取topic的所有分区
    val topicAndPartition = cluster.getPartitions(topics)

    topicAndPartition match {
      // 1 获取这些topic的所有分区
      case Right(topicAndPartitionSet) =>
        val value = cluster.getConsumerOffsets(groupId, topicAndPartitionSet)
        value match {
          //每个topic的每个分区都已经存储过偏移量 表示曾经消费国 而且也维护这个偏移量
          case Right(map) => {
            resultMap ++= map
          }
          // 表示这个topic的这个分区是第一次消费
          case _ =>
            topicAndPartitionSet.foreach(topicAndPa1 => {
              resultMap += (topicAndPa1 -> 0L)
            })
        }
      case _ =>
    }
    resultMap
  }

  def saveOffsets(stream:InputDStream[String]) = {
    //保存offset一定从kafka消费到的直接的那个stream保存
    //每个批次执行一次传递过去的函数
    stream.foreachRDD(rdd =>{
      var map = Map[TopicAndPartition,Long]()
      //如果这个rdd时候是直接来自于kafka 则可以强制转成HasOffsetRanges
      //这个类型包含了 这次消费的offset信息
      val hasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      //所有分区的偏移量
      val ranges = hasOffsetRanges.offsetRanges
      ranges.foreach(offSetRange => {
        val key = offSetRange.topicAndPartition()
        val value = offSetRange.untilOffset
        map += key -> value
      })
      cluster.setConsumerOffsets(groupId,map)
    })
  }

  def main(args: Array[String]): Unit = {
    //采用读取offset的方式

    val conf = new SparkConf().setAppName("c4").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    //开始创建和kafka相关的
    val sourceStream = KafkaUtils.createDirectStream[String, String,StringDecoder, StringDecoder, String](
      ssc,
      params,
      readOffsets(),
      (handler: MessageAndMetadata[String, String]) => handler.message()
    )
    sourceStream
      .flatMap(_.split("\\W+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print(1000)

    saveOffsets(sourceStream)
    ssc.start()
    ssc.awaitTermination()
  }
}
