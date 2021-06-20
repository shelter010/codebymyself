package com.hpf.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/4/17 下午12:10  
 * @Version 1.0
 */
object SparkSql2_input {
  def main(args: Array[String]): Unit = {
    import org.apache.spark
    //通过rdd转换
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //rdd获取数据
    val rdd = sc.textFile("/Users/mac/Users/hpf/sparkStudy/spark1015/spakSql/input/user.txt")
    val UserRdd = rdd.map(line=>{
      val arr = line.split(",")
      (arr(0),arr(1).trim.toInt)
    })

    val User1Rdd = rdd.map(line=>{
      val arr = line.split(",")
      User(arr(0),arr(1).toInt)
    })
    //创建spark session
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //导入隐士转换包
    import spark.implicits._
    val user1DF = User1Rdd.toDF()
    UserRdd.toDF("name","age").show()
    println("=======")
    User1Rdd.toDF().show()

    val rdd1 = user1DF.rdd
    //4.关闭连接

    sc.stop()

  }
}

case class User(name:String,age:Int)