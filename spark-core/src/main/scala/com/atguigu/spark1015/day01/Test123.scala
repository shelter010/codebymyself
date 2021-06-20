package com.atguigu.spark1015.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/4/14 上午9:57  
 * @Version 1.0
 */
object Test123 {
  def main(args: Array[String]): Unit = {
    //创建1个rdd 实现每个元素和分区号形成1个元组
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4), 2)
    //3 用mapPartitionWithIndex 带上索引
    val resRdd = rdd1.mapPartitionsWithIndex((index, items) => {
      items.map((index, _))
    })

    // 转换为数组 然后输出
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    //用到glom方法 将每一个分区的数据转换为数组
    rdd2.glom().mapPartitionsWithIndex((index, dataS) => {
      println(index + "号分区--" + dataS.next().mkString(","))
      dataS
    }).collect()

    // 求每个分区最大值
    val res = rdd2.glom().mapPartitionsWithIndex((index, dataS) => {
      dataS.map(arr => {
        (index, arr.max)
      })
    })

    println(res.reduce((v1, v2) => {
      (0, v1._2 + v2._2)
    }))

    val ccc = rdd2.glom().map(_.max)
    println(ccc.collect().sum)

    println("==="*3)
    //groupBy
    val rdd3 = sc.makeRDD(List("hello","hive","hadoop","spark","scala"))
    //rdd3.collect().foreach(println)
    //按照首字母相同的分到同一组
    rdd3.groupBy(str=>{str.substring(0,1)})
     //   .collect().foreach(println)



    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    //4.1 缩减分区
    val coalesceRdd: RDD[Int] = rdd.coalesce(2)

    //5 打印查看对应分区数据
    val indexRdd: RDD[Int] = coalesceRdd.mapPartitionsWithIndex(
      (index, datas) => {
        // 打印每个分区数据，并带分区号
        datas.foreach(data => {
          println(index + "=>" + data)
        })
        // 返回分区的数据
        datas
      }
    )
    indexRdd.collect()

    // resRdd.collect().foreach(println)
    //
    //4.关闭连接
    sc.stop()

  }
}
