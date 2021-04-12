package com.atguigu.spark1015.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/17 上午10:31  
 * @Version 1.0
 */
object AggregateDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AggregateDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)))
//    //分区内求相同key的最大值 分区间求和 需要用到AggregateByKey
//    val rdd2 = rdd1.aggregateByKey(0)((u, v) => u.max(v), (u1, u2) => u1 + u2)
//    rdd2.collect().foreach(println)

    //在上面的基础上 同时求出最大的和和最小的和 也是同一个key 可以用元组
//    val rdd2 = rdd1.aggregateByKey((Int.MinValue, Int.MaxValue))({
//      case ((u1, u2), v) => (u1.max(v), u2.min(v))
//    }, {
//      case ((u1, u2), (u11, u21)) => (u1 + u11, u2 + u21)
//    })

    //计算每个key平均值 计算每个key的和 计算key的个数
    val rdd2 = rdd1.aggregateByKey((0, 0))(
      {
        case ((u1, u2), v) => (u1 + v, u2 + 1)
      }
      , {
        case ((u11, u22), (u111, u222)) => (u11 + u111, u22 + u222)
      }).map{
      case (k,(sum,count)) => (k,sum.toDouble / count)
    }
    //或者 用mapValues 这时候key不需要动的了
    // .mapValues(sum,count) => sum.toDouble / count
    rdd2.collect().foreach(println)
    sc.stop()

  }
}
