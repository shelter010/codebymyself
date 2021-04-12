package com.atguigu.streaming.project

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/3/3 下午7:38  
 * @Version 1.0
 */
object MapJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("aab2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("hello", "word", "world", "hello", "hpf", "hello")).map((_, 1))
    val rdd2 = sc.parallelize(Array("world", "hpf", "hello")).map((_, 1))
    //避免使用reduceByKey以及其他的shuffle算子
    //    val rdd3 = rdd1.join(rdd2)
    //    rdd3.collect.foreach(println)
    /*
                                                                                   (hpf,(1,1))
(hello,(1,1))
(hello,(1,1))
(hello,(1,1))
(world,(1,1))
上面有join 实现上面的过程 可以不用shuffle
    */
    //可以通过将小的rdd广播出去 然后大的rdd进行map聚合
    val bd = sc.broadcast(rdd2.collect())
    val res = rdd1.flatMap {
      case (k, v) =>
        val data = bd.value
        data.filter(_._1 == k).map {
          case (k2, v2) => (k2, (v, v2))
        }

    }

    res.collect().foreach(println)
  }
}
