package com.atguigu.streaming.project.util

import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

/**
 * @author hpf
 * @create 2021/3/2 下午11:06  
 * @Version 1.0
 */
object RealUtil {
   implicit class MyRedis(stream: DStream[((String,String),List[(String,Int)])]){
     def saveToRedis = {
       stream.foreachRDD(rdd => {
               rdd.foreachPartition(it => {
                 //1 建立到redis的连接
                 val client = RedisUtil.getClient
                 //2 写数据到redis
                 it.foreach {
                   case ((day, area), (adsCountList)) =>
                     val key = "area:ads:count" + day
                     val field = area
                     //把集合转换json字符串 json4s
                     //专门用于把集合转换成json字符串 （样例类不行）
                     import org.json4s.JsonDSL._
                     val value = JsonMethods.compact(JsonMethods.render(adsCountList))
                     client.hset(key, field, value)
                 }
                 //关闭到redis的连接
                 client.close() //其实就是把这个客户端还给连接池


                 //3 关闭到redis的连接

               })
             })
     }
   }
}
