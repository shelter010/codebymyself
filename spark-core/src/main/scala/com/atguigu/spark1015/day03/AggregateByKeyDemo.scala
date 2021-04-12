package com.atguigu.spark1015.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/7 下午11:42  
 * @Version 1.0
 */
object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("A_Demo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    //需求 计算每个分区相同key最大值 然相加 ；分区内计算最大值 分区间是求和
    //val rdd2 = rdd1.aggregateByKey(Int.MinValue)((u, v) => u.max(v), (u1, u2) => u1 + u2)
    //上面的代码 简化的时候是下面的
    //    val rdd2 = rdd1.aggregateByKey(Int.MinValue)(_.max(_),_+_)
    //    val rdd3 = rdd1.aggregateByKey(Int.MaxValue)(_.min(_), _ + _)
    //    //同时求出来最大值的和 以及最小值的和
    //    rdd2.collect.foreach(println)
    //    println("---------")
    //    rdd3.collect.foreach(println)

    //上面不规划 不是同时计算最大和最小值，下面的方法是可以的 运用到了偏函数
    //    val result = rdd1.aggregateByKey((Int.MinValue,Int.MaxValue))(
    //      //用大括号表示出来的就是偏函数
    //      {
    //        case ((max,min),v) => (max.max(v),min.min(v))
    //      },
    //      {
    //        case ((max1,min1),(max2,min2)) => (max1+max2,min1+min2)
    //      }
    //    )
    //
    //    result.collect().foreach(println)

    //计算每个key的平均值 a：每个key的value和  b：计算每个key出现的次数
    val result = rdd1.aggregateByKey((0, 0))(
      {
        case ((sum, count), v) => (sum + v, count + 1)
      },
      {
        case (( sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
      }
    ).map(
      {
        case (k,(sum,count)) => (k,sum.toDouble / count)
      }
      //上面的或者可以直接用下面的方式
      /**
       *  .mapValues{
       *    case (sum,count) =>sum.toDouble / count
       *  }
       */
    )

    //在上面的基础上再求平均年值 可以用map
    result.collect().foreach(println)
    sc.stop()
  }
}
