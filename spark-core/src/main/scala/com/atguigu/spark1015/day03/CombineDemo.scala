package com.atguigu.spark1015.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/17 下午3:08  
 * @Version 1.0
 */
object CombineDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("combineDemo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)))

//    //对其中的每个key 进行求和 用这个方式
//    val rdd2 = rdd1.combineByKey(
//      v => v,
//      (c: Int, v: Int) => c + v,
//      (c1: Int, c2: Int) => c1 + c2
//    )


//    //每个key在每个分区内的最大值 然后在求出这些最大值的和
//    val rdd2 = rdd1.combineByKey(
//      v => v,
//      (c: Int, v: Int) => c.max(v),
//      (c1: Int, c2: Int) => c1 + c2
//    )

    //求解每个key的平均值
    val rdd2 = rdd1.combineByKey(
      v => (v, 1),
      (sumCount: (Int, Int), v) => (sumCount._1 + v, sumCount._2 + 1),
      (sumCount1: (Int, Int), sumCount2: (Int, Int)) => (sumCount1._1 + sumCount2._1, sumCount1._2 + sumCount2._2)
    ).map(
      {
        case (key,count:(Int,Int)) => (key,count._1.toDouble / count._2)
      }
    )
    rdd2.collect.foreach(println)

    sc.stop()
  }
}
