package com.atguigu.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author hpf
 * @create 2021/6/10 下午6:45  
 * @Version 1.0
 */
object sparkSqlTest {
  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")
    //    //常见dataFrame
    //    val spark = SparkSession.builder().config(conf).getOrCreate()
    //
    //    val df = spark.read.json("/Users/mac/Users/hpf/sparkStudy/spark1015/spark-core-project/src/main/resources/user.json")
    //    df.show()
    //    spark.stop()
    //        val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    //        val sparkContext = new SparkContext(conf)
    //        val LineRdd = sparkContext.textFile("/Users/mac/Users/hpf/sparkStudy/spark1015/spark-core-project/src/main/resources/user.txt")
    //        //rdd准备完成
    //        val UserRDD = LineRdd.map {
    //          x =>
    //            val fields = x.split(",")
    //            (fields(0), fields(1).trim.toInt)
    //        }
    //        //创建sparksession对象
    //        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    //        //rdd 和 df 以及ds导入 必须需要导包
    //        import sparkSession.implicits._
    //       // UserRDD.toDF("name","age").show()
    //
    //        val resDF = UserRDD.map(x => User(x._1, x._2)).toDS()
    //        resDF.show()
    //
    //        //rd he ds transform
    //      //  UserRDD.toDS()
    //        sparkContext.stop()
    val conf = new SparkConf().setAppName("aaa").setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    //    val df = sparkSession.read.format("jdbc")
    //      .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .option("user", "root")
    //      .option("password", "123456")
    //      .option("dbtable", "account")
    //      .load()
    //
    //    df.show()
    //    sparkSession.stop()

    //mysql 写数据
    import sparkSession.implicits._
    val rdd = sparkSession.sparkContext.makeRDD(List(account(5,"jerry", 3356)))
    val ds = rdd.toDS()
    //
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "account")
      .mode(SaveMode.Append)
      .save()
    sparkSession.stop()
  }

}

case class User(name: String, age: Int)

case class account(id:Int,username: String, balance: Int)