package com.hpf.sparkstreaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/4/20 上午9:58  
 * @Version 1.0
 */
object ReduceByKeyAndWindowT {
  def main(args: Array[String]): Unit = {

    //创建配置文件对象
    //注意：Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    //需要设置检查点 因为有2个状态 需要保存状态
    ssc.checkpoint("/Users/mac/Users/hpf/sparkStudy/spark1015/ck")
    val inDStream = ssc.socketTextStream("master",7777)

    //使用 反向函数
     val res: DStream[(String, Int)] = inDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKeyAndWindow(
        //当前窗口的计算逻辑
        (x: Int, y: Int) => (x + y),
        //当前窗口的内状态-滑出窗口的值 =得到的结果是当前这个窗口状态内的值
        (x: Int, y: Int) => x - y,
        //窗口相关内容 滑动窗口长度和滑动步长都是采集周期整数倍
        Seconds(12),
        Seconds(6),
        new HashPartitioner(2), //分区器
        //wcount => wcount._2> 0 //过滤条件
        (x:(String,Int)) => x._2>0
      )

    res.print()
    //启动采集器。 采集器只是一个守护线程，放在print后面可以。
    ssc.start()
    //等待采集结束后终止程序
    ssc.awaitTermination()
  }



}
