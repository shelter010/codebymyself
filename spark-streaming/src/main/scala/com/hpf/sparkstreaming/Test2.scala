package com.hpf.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author hpf
 * @create 2021/4/19 下午12:45  
 * @Version 1.0
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    //注意：Streaming程序执行至少需要2个线程，一个用于接收器采集数据，一个用于处理数据，所以不能设置为local
    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //自定义数据源 接收到socket master 7766的数据
    val lineDStream = ssc.receiverStream(new MyReceiver1("master", 7777))

    val resDStream = lineDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    //启动采集器。 采集器只是一个守护线程，放在print后面可以。
    resDStream.print()
    ssc.start()
    //等待采集结束后终止程序
    ssc.awaitTermination()

  }
}

class MyReceiver1(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  //
  private var socket: Socket = _ //网络套接字
  override def onStart(): Unit = {
    //需要新开一个线程采集数据
    new Thread("socket receiver") {
      //将当前线程设置为守护线程
      setDaemon(true)

      override def run(): Unit = {
        receiver2()
      }
    }.start()

    def receiver2(): Unit = {
      socket = new Socket(host, port)
      //设置一个bufferReader缓存socket数据 读取端口传来的数据
      val reader = {
        new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      }
      //读取数据 -- 者至少是一条数据
      var input = reader.readLine()
      //当receiver没有关闭并且输入数据不为空 则循环发送数据给spark

      while (!isStopped() && input != null) {
        store(input)
        input = reader.readLine()
      }
      //如果循环结束
//      reader.close()
//      socket.close()

      //重启接收任务
      restart("restart")
    }
  }

  override def onStop(): Unit = {
//    syschronized{
//
//    }
    if (socket != null) {
      socket.close() //释放资源
      socket = null

    }
  }
}
