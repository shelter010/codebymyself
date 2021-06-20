package com.hpf.sparksql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/4/18 下午10:24  
 * @Version 1.0
 */
object TestMysqlReadAndWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    //sparkSession是df以及spark sql的入口
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //测试向mysql数据库中读写数据

    //1 从sql读取数据 那么数据源就是jdbc
    val df = spark.read.format("jdbc")
      //四要素
      .option("url", "jdbc:mysql://master:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load()

    //方法2 因为option是键值对 所以可以想到options里面直接是map
    val df1 = spark.read.format("jdbc")
      .options(Map(
        "url" -> "jdbc:mysql://master:3306/test?user=root&password=123456",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "user"
      ))
      .load()

    //方法3 直接通过jdbc 类似读取json方法来读取
    val pros = new Properties()
    pros.setProperty("user","root")
    pros.setProperty("password","123456")
//    spark.read.jdbc("jdbc:mysql://master:3306/test?user=root" +
//      "&password=123456","user",null)
val df2 = spark.read.jdbc("jdbc:mysql://master:3306/test", "user", pros)

    //向其中写数据 df.write.save
    //还是按照ds的方式来写  将rdd转换为ds 2方式 一种是样例类 一种是元组
    val rdd = spark.sparkContext.makeRDD(List(User1(4,"libai",99),User1(5,"dufu",98),User1(6,"liubei",57)))
    //将rdd转换为ds -》这里需要用到隐式转换
    import spark.implicits._
    val ds = rdd.toDS()
    ds.write.format("jdbc")
      .options(Map(
        "url"->"jdbc:mysql://master:3306/test?user=root&password=123456",
        "driver"->"com.mysql.jdbc.Driver",
        "dbtable"->"user"
      ))
      .mode("append")
        .save()


    //df.show()
    spark.close()
  }
}

case class User1(id: Int, name: String, age: Int)

