package com.hpf.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/4/20 上午9:07  
 * @Version 1.0
 */
object UpstateT {
  //定义更新状态的方法 seq为当前批次单词次数 state为以往批次单词次数
  val updateFunc = (seq: Seq[Int], state: Option[Int]) => {
    //当前批次数据的累加
    val currentCount = seq.sum
    val preCount = state.getOrElse(0)
    //累加
    Some(currentCount + preCount)
  }

  def createSsc(): StreamingContext = {
  //创建spatkconf
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")
    //
    val ssc = new StreamingContext(conf,Seconds(3))

    //设置检查点
    ssc.checkpoint("./ck")
    //获取一行数据
    val lines = ssc.socketTextStream("master",7777)
    //切割
    val words = lines.flatMap(_.split(" "))
    val res = words.map(word =>(word,1))
      .updateStateByKey[Int](updateFunc)
      .print()
      ssc




  }

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate("./ck", () => createSsc())
    ssc.start()
    ssc.awaitTermination()
  }
}
