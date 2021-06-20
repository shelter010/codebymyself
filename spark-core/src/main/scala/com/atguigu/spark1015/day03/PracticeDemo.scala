package com.atguigu.spark1015.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/17 下午9:15  
 * @Version 1.0
 */
object PracticeDemo {
  def main(args: Array[String]): Unit = {
    /**
     * 数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割
     *
     * 得出每个身份广告点击的top3 并输出
     * 分析：
     * rdd【点击记录】 做map操作
     * rdd（（pro，广告），1） reduceByKey操作
     * rdd（（pro，广告），10） map操作
     * rdd(pro,(广告，10)) 做groupByKey ，这样就能够按照省份进行分组
     * rdd（pro，iterable（(广告1，10）,（广告2，9），（广告3，6）...）做map 只对内部的list排序，取前3
     * rdd（pro，iterable（(广告1，10）,（广告2，9），（广告3，6））
     */
    val conf = new SparkConf().setAppName("PracticeDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val rdd1 = sc.textFile("/Users/mac/Downloads/download from mac/download from baiduwangpan/15-spark/agent.log")
    val rdd1 = sc.textFile("/Users/mac/Downloads/personal/agent.log")
    //    val provinceAdOne = rdd1.map(line => {
    //      val splits = line.split(" ")
    //      ((splits(1), splits(4)), 1)
    //    })
    //    val provinceAdCount = provinceAdOne.reduceByKey(_ + _)
    //    val provinceAdCountT = provinceAdCount.map({
    //      case ((pro, ad), count) => (pro, (ad, count))
    //    })
    //    val provinceGrouped = provinceAdCountT.groupByKey()
    //
    //    val res = provinceGrouped.map({
    //      //元组适合偏函数 所以这里偏函数 下面需要用元祖
    //      case (pro, adCount) =>
    //        //转换成容器式才能排序
    //        (pro, adCount.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
    //    })
    val rdd2 = rdd1.map(line => {
      val splits = line.split(" ")
      ((splits(1), splits(4)), 1)
    })
    val rdd3 = rdd2.reduceByKey(_ + _)

    val rdd4 = rdd3.map {
      case ((pro, ad), count) => (pro, (ad, count))
    }

    val rdd5 = rdd4.groupByKey()

    val res = rdd5.map({
      case (pro, adCount) => {
        (pro, adCount.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
      }
    })

    //优化一下 按照key进行排序 考虑把key变成int之后再排序
    val r = res.sortBy(_._1.toInt)
    r.collect().foreach(println)
    sc.stop()
  }
}
