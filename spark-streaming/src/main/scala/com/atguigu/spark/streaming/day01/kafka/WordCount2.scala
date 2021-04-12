package com.atguigu.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/2/28 下午7:22  
 * @Version 1.0
 */
object WordCount2 {

  def createSSC(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("a")
    val ssc = new StreamingContext(conf, Seconds(3))

    //把offset的根据在检查点中   这个十分关键
    ssc.checkpoint("chk1")
    //从kafka读取数据
    val param = Map[String, String](
      "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
      "group.id" -> "1015"
    )
    //通过kafkaUtil来进行创建 直连的方式
    val sourceStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      param,
      Set("first1015")
    )

    //对数据进行处理
    sourceStream.flatMap {
      case (_, v) =>
        v.split("\\W+")
    }
      .map((_, 1))
      .reduceByKey(_+_)
        .print()
        //

    //最后返回ssc
        ssc
  }

  def main(args: Array[String]): Unit = {
    /*
    从检查点中 恢复一个额streamingContext 如果检查点不存在 则调用后面的函数去
    创建一个streamingContext
     */
    val ssc = StreamingContext.getActiveOrCreate("chk1", createSSC)

    ssc.start()
    ssc.awaitTermination()
  }
}
