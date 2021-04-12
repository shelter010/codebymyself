package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/2/26 上午7:44  
 * @Version 1.0
 */
object CreateDS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("DF2DD").master("local[2]").getOrCreate()
    val list = List(30, 34, 45, 5, 65, 6, 4, 21)

    //需要加上隐式转换
    import spark.implicits._
//    //把集合转换为ds
//    val ds = list.toDS()
//    ds.show()
    //存储样例类的情况多
val list1 = List(User("aa", 1), User("bb", 3), User("cc", 5))
    val ds = list1.toDS()

    //在ds做sql查询
    ds.createOrReplaceTempView("user")
    spark.sql("select * from user where age > 3").show()
    ds.show()
    spark.close()
  }
}
