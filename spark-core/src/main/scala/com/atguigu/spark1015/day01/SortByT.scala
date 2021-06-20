package com.atguigu.spark1015.day01

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/4/14 下午1:36  
 * @Version 1.0
 */
object SortByT {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3 order
    val rdd1 = sc.parallelize(List(1, 4, 3, 2, 8, 6, 9), 2)
    rdd1.mapPartitionsWithIndex((index, data) => {
      data.map(x => (index, x))
    }).collect().foreach(println)
    println("=======")
    val rdd2 = rdd1.sortBy(num => num)
    val rdd3 = rdd2.mapPartitionsWithIndex((index, dataS) => {
      dataS.map((x => (index, x)))
    })
    rdd3.collect().foreach(println)

    //
    println("===="*4)
    val rdd111 = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"),(5,"ddd")), 3)
    //对rdd进行重新分区
    val rdd222 = rdd111.partitionBy(new org.apache.spark.HashPartitioner(2))
    //打印查看对应对分区数据
    rdd222.mapPartitionsWithIndex((index,x)=>{
      x.map(a =>(index,a))
    }).collect().foreach(println)


    //自定义分区器
    val rdd333 = rdd111.partitionBy(new MyPar(2))
    rdd333.collect().foreach(println)
    //4.关闭连接
    sc.stop()

  }
}

class MyPar(n:Int) extends Partitioner {
  override def numPartitions: Int = n

  override def getPartition(key: Any): Int = {
    if(key.isInstanceOf[Int]){
      val keyInt = key.asInstanceOf[Int]
      if (keyInt % 2 ==0) 0
      else 1
    }else 0
  }
}
