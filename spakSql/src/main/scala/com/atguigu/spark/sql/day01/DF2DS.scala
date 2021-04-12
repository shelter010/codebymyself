package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/2/26 上午8:20  
 * @Version 1.0
 */
case class Person(name:String,age:Long)
object DF2DS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("qw1").getOrCreate()
   //从df 到 ds 转换 旧的到新的 需要隐式转换
    import spark.implicits._

    val df = spark.read.json("/Users/mac/Downloads/download from mac/download from baiduwangpan/15-spark/spark-sql数据/user.json")
//as 后面传入的是样例类
    val ds = df.as[Person]
 //   ds.show()

    //ds 转 df
    val df1 = ds.toDF()
    df1.show()
    spark.close()
  }
}
