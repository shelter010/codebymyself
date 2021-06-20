package com.atguigu.spark1015.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/4/14 下午4:29  
 * @Version 1.0
 */
object CombineByKeyT {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3 用combineByKey实现分求和和平均值
    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val rdd1 = sc.parallelize(list, 2)
    val combinedRdd = rdd1.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    combinedRdd.collect().foreach(println)

    //        求解平均值
    combinedRdd.map(data =>{
      (data._1,data._2._1/data._2._2.toDouble)
    }).collect().foreach(println)

    println("下面是groupByKey来实现的")
    rdd1.groupByKey().map(data => (data._1,data._2.sum / data._2.size.toDouble))
        .collect().foreach(println)


    val rdd333: RDD[(String, Int)] = sc.makeRDD(List(("Bob", 89), ("Ailce", 92), ("Bob", 75), ("Ailce", 82), ("Bob", 62), ("Ailce", 73)), 2)
    //求解每个同学的平均分 使用combineByKey求解 试试 转换数据结构后 对分区内和分区见进行聚合
    val rdd4 = rdd333.combineByKey(
      (_, 1),
      (elem1: (Int, Int), elem2: Int) => (elem1._1 + elem2, elem1._2 + 1),
      (elem1: (Int, Int), elem2: (Int, Int)) => (elem1._1 + elem2._1, elem1._2 + elem2._2)
    )
    //求平均分
    rdd4.map({
      case (name,score:(Int,Int)) =>(name,score._1 /score._2.toDouble)
    }).collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}
