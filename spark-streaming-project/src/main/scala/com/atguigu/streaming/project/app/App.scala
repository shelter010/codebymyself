package com.atguigu.streaming.project.app


import com.atguigu.streaming.project.bean.AdsInfo
import com.atguigu.streaming.project.util.MyKafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author hpf
 * @create 2021/3/1 下午10:46
 * @Version 1.0
 */
trait App {
  //写一些公共的代码
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("aaa").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ckc1015")
    val stream = MyKafkaUtils.getKafkaStream(ssc, "ads_log1015")

    val adsInfoStream = stream.map(s => {
      val splits = s.split(",")
      AdsInfo(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
    })

    doSomething(adsInfoStream)

    //stream.print()


    ssc.start()
    ssc.awaitTermination()
  }

  def doSomething(adsInfoStream: DStream[AdsInfo]): Unit

}
