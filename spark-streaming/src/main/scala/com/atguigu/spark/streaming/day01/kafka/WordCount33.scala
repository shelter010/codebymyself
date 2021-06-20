//package com.atguigu.spark.streaming.day01.kafka
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
// * @author hpf
// * @create 2021/2/28 下午8:24
// * @Version 1.0
// */
//object WordCount33 {
//
//  val groupId = "1015"
//
//  val params = Map[String, String](
//    "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
//    "group.id" -> groupId
//  )
//  // kafkaUtils kafkaCluster
//  val cluster = new KafkaCluster(params)
//
//  val topics = Set("first1015")
//
//  def readOffsets() = {
//    var resultMap = Map[TopicAndPartition, Long]()
//    //获取topic的所有分区
//    val topicAndPartition = cluster.getPartitions(topics)
//
//    topicAndPartition match {
//      // 1 获取这些topic的所有分区
//      case Right(topicAndPartitionSet) =>
//        val value = cluster.getConsumerOffsets(groupId, topicAndPartitionSet)
//        value match {
//          //每个topic的每个分区都已经存储过偏移量 表示曾经消费国 而且也维护这个偏移量
//          case Right(map) => {
//            resultMap ++= map
//          }
//          // 表示这个topic的这个分区是第一次消费
//          case _ =>
//            topicAndPartitionSet.foreach(topicAndPa1 => {
//              resultMap += (topicAndPa1 -> 1L)
//            })
//        }
//      case _ =>
//    }
//    resultMap
//  }
//
//  def main(args: Array[String]): Unit = {
//    //采用读取offset的方式
//
//    val conf = new SparkConf().setAppName("cd").setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Seconds(2))
//
//    //开始创建和kafka相关的
//    val sourceStream = KafkaUtils.createDirectStream[String, String,StringDecoder, StringDecoder, String](
//      ssc,
//      params,
//      readOffsets(),
//      (handler: MessageAndMetadata[String, String]) => handler.message()
//    )
//    sourceStream
//      .flatMap(_.split("\\W+"))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//      .print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
