package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/3/1 上午8:02  
 * @Version 1.0
 */
object UpdateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("abcd")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck1")

    ssc
      .socketTextStream("master",9999)
      .flatMap(_.split("\\W+"))
      .map((_,1))
      .updateStateByKey((seq:Seq[Int],opt:Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })
      .print(1000)


    ssc.start()
    ssc.awaitTermination()
  }
}
