package com.hpf.sparkstreaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @author hpf
 * @create 2021/4/20 上午11:13  
 * @Version 1.0
 */

/**
 * 从套接字读取文件 然后设置优雅关闭 这里使用updatestatebykey（func） 开始就对key相同的value累加
 */
object GrafullyShut {

  val funcU: (Seq[Int], Option[Int]) => Some[Int] = (seq: Seq[Int], option: Option[Int]) => {
    val currentCount = seq.sum
    val preCount = option.getOrElse(0)
    Some(currentCount + preCount)
  }

  //定义一个createssc方法 因为防止checkpoint之后无法从checkpoint读取
  def createSc(): StreamingContext = {
    /**
     * ds转换的逻辑 以及创建ssc
     */
    val conf = new SparkConf().setAppName("aaa").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    //设置优雅关闭
    conf.set("spark.streaming.stopGracefulltShutDown", "true")

    //设置检查点
    ssc.checkpoint("./hpfck") //下面是ds转换逻辑 统计wordcount
    val inputDStream = ssc.socketTextStream("master", 8888)
    //进行转换
    inputDStream.flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey(funcU)
      .print()

    ssc

  }

  def main(args: Array[String]): Unit = {
   // System.setProperty("HADOOP_USER_NAME", "root")
    //通过另外一种方式获取ssc
    val sc = StreamingContext.getActiveOrCreate("./hpfck", () => createSc())

    //启动线程进行 设置标记位 进行优雅关闭判断  ---也就是监控程序
    new Thread(new MonitorStop(sc)).start

    sc.start()
    sc.awaitTermination()
  }
}

class MonitorStop(sc: StreamingContext) extends Runnable {
  //因为是分布式的 所以执行程序的executor会在不同的机器节点上 所以可以在分布式文件系统上设置一个
  //标识位 这样所有的executor都可以访问到
  override def run(): Unit = {

    // 1 定义监控的路径
    val fs = FileSystem.get(new URI("hdfs://master:9000"), new Configuration()
      , "root")

    //2  设置一个无限循环操作 这个也是新启动一个线程也监控标识位
    while (true) {
      //设置一个检测时间间隔 5s更新检测一次hdfs上标识位的变化情况
      try {
        Thread.sleep(5000)
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }

      //3 定义标识位
      val res = fs.exists(new Path("hdfs://master:9000/stopSpark"))
      if (res) {
        val state = sc.getState()
        if (state == StreamingContextState.ACTIVE) {
          sc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }


    }
  }
}

