package com.atguigu.spark.streaming.day01

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 * @author hpf
 * @create 2021/2/28 下午1:57  
 * @Version 1.0
 */
object MyReceiverDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Myr")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceStream = ssc.receiverStream(new MyReceiver("master", 8989))
    sourceStream.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}

class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  var socket:Socket = _
  var reader:BufferedReader = _
  override def onStart(): Unit = {
    onInThread{
      try {
        socket = new Socket(host, port)
        //流
        reader = new BufferedReader((new InputStreamReader(socket.getInputStream, "utf-8")))
        var line = reader.readLine()
        while (line != null && socket.isConnected) {
          store(line)
          line = reader.readLine()
        }
      }catch {
        case e => e.printStackTrace()
      }finally {
        restart("重启接收器")
        //自动立即调用onStop 然后再调用onStart
      }
    }
  }

  def onInThread(op: => Unit): Unit ={
    new Thread(){
      override def run(): Unit = op
    }.start()
  }
  //释放资源
  override def onStop(): Unit = {
    if(socket != null) socket.close()
    if (reader != null) reader.close()
  }
}
