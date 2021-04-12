package com.atguigu.streaming.project.app

import com.atguigu.streaming.project.bean.AdsInfo
import com.atguigu.streaming.project.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

/**
 * @author hpf
 * @create 2021/3/1 下午10:47  
 * @Version 1.0
 */
object AreaTopApp extends App {
  override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
    //adsInfoStream.print(1000)
    //每天每地区的top3广告点击量
    val dayAreaGrouped = adsInfoStream.map(info => {
      ((info.dayString, info.area, info.adsId), 1)
    })
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })
      .map {
        case ((day, area, ads), count) => ((day, area), (ads, count))
      }
      .groupByKey()

    //每组内进行排序取top3
    val res = dayAreaGrouped.map {
      case (key, it) => {
        (key, it.toList.sortBy(-_._2).take(3))
      }
    }

    // 这是测试代码 res.print(100)
    //把数据写入到redis中
//    res.foreachRDD(rdd => {
//      rdd.foreachPartition(it => {
//        //1 建立到redis的连接
//        val client = RedisUtil.getClient
//        //2 写数据到redis
//        it.foreach {
//          case ((day, area), (adsCountList)) =>
//            val key = "area:ads:count" + day
//            val field = area
//            //把集合转换json字符串 json4s
//            //专门用于把集合转换成json字符串 （样例类不行）
//            import org.json4s.JsonDSL._
//            val value = JsonMethods.compact(JsonMethods.render(adsCountList))
//            client.hset(key, field, value)
//        }
//        //关闭到redis的连接
//        client.close() //其实就是把这个客户端还给连接池
//
//
//        //3 关闭到redis的连接
//
//      })
//    })
    import com.atguigu.streaming.project.util.RealUtil._
    res.saveToRedis


  }
}

///*
// * redis数据类型
// * k-v 形式数据库（nosql数据）
// * k： 都是字符串
// * v：的数据类型
// * 5大数据类型
// * 1 string
// * 2 set 不重复
// * 3 list 允许重复
// * 4 hash map 存的是filed value
// * 5 zset
// * -----------
// * (2021-03-02,华南),List((3,17791), (2,17507), (1,17455)))
// * ((2021-03-02,华东),List((1,17676), (2,17606), (5,17475)))
// * ((2021-03-02,华中),List((5,5891), (3,5854), (1,5837)))
// * ((2021-03-02,华北),List((2,17865), (4,17699), (5,17595)))
// * 存储到redis
// * 选择什么数据类型
// * 每天1个key
// * key                               value
// * "area:ads:count" + day            hash
// * field    value
// * area     json字符串
// * "华南"   {3:17791,2:17507,1:17455}
// *
// * /


