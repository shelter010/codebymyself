package com.atguigu.streaming.project.app

import com.atguigu.streaming.project.bean.AdsInfo
import com.atguigu.streaming.project.util.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

/**
 * @author hpf
 * @create 2021/3/3 上午9:30  
 * @Version 1.0
 */
object LastHourTop extends App {
  override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
    adsInfoStream
      .window(Minutes(60),Seconds(3))
      .map(info => ((info.adsId,info.hmString),1))
      .reduceByKey(_+_)
      //再按照广告分组 把这个广告下面的所有分钟记录放在一起
      .map{
        case ((ads,hm),count) => (ads,(hm,count))
      }
      .groupByKey()
     // .print(1000)
    //写入到redis中
      .foreachRDD(rdd =>{
        rdd.foreachPartition(it => {
         //要用下面这个
          if(it.hasNext){   //  或者用it.noEmpty 这个底层也是hasNext方法
          // 不对 if (it.size > 0) {
            //1 建立到redis连接
            val client = RedisUtil.getClient
            //2 写入到redis
            //1 一个一个到写
            //2 批次写入 这次采用批次写入
            //需要导入一个东西
            import org.json4s.JsonDSL._
            val key = "last:ads:count"
            //通过jsonMethod方法 将集合转换为json字符串
            val map = it.toMap.map {
              //case (ads, it) => (ads, JsonMethods.compact(JsonMethods.render(it)))
              case (ads, it) => (ads, JsonMethods.compact(JsonMethods.render(it)))
            }
            //scala集合转换为java集合
            import scala.collection.JavaConversions._
          //  println(map)
            client.hmset(key, map)

            //3关闭redis连接 用的是连接池 实际是把连接池归还给连接池
            client.close()
          }
        })

      })

  }
}

/*
统计各广告最近1小时的点击量趋势 各广告最近1小时内各分钟的点击量 每60秒统计一次


1 key       value
  广告id       json字符串每分钟点击


 2 key                           value
    "last:ads:hour:count"        hash
                                 field           value
                                 adsId           json字符串
                                 "1"             {"09:24":100,"09:25":110,...}
 迭代器 里面的每个元素只能访问一次 之后指针指到最后的位置
    iterator 是一个迭代器   ；iterable 表示的是可叠代的对象 没啥问题
 */