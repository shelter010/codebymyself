package com.atguigu.streaming.project.app

import com.atguigu.streaming.project.bean.AdsInfo
import com.atguigu.streaming.project.util.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

/**
 * @author hpf
 * @create 2021/3/3 下午2:01  
 * @Version 1.0
 */
object LastTopApp1 extends App {
  override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
    adsInfoStream
      .window(Minutes(60), Seconds(3))
      .map(info => ((info.adsId, info.hmString), 1))
      .reduceByKey(_ + _)
      //再按照广告进行分组
      .map {
        case ((adsId, hm), count) => (adsId, (hm, count))
      }
      .groupByKey()
      //写入到redis中
      .foreachRDD(rdd => {
        //建立到redis的连接
        val client = RedisUtil.getClient
        //向redis中写入数据
        rdd.foreachPartition(it => {
          if (it.hasNext) {
            import scala.collection.JavaConversions._
            import org.json4s.JsonDSL._
            val key = "ads:min:count"

            val map = it.toMap.map({
              case (ads, hmAndCount) => (ads, JsonMethods.compact(JsonMethods.render(hmAndCount)))
            })

            client.hmset(key, map)

            client.close()
          }
        })

        //3 关闭redis的连接
      })
  }
}

/** *
 *
 * 统计各广告最近1小时的点击量趋势 各广告最近1小时内各分钟的点击量 每60秒统计一次
 *
 * day area  adsIdCount
 *
 * key
 * "day:area-count"    value
 * hash
 * field  value
 * adsId   json字符串
 */
