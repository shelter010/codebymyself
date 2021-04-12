package com.atguigu.spark.streaming.day02

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author hpf
 * @create 2021/3/1 下午8:09  
 * @Version 1.0
 */
object OutDemo1 {
  def main(args: Array[String]): Unit = {
    val pros = new Properties()
    pros.setProperty("user", "root")
    pros.setProperty("password", "123456")
    //将socket中输入的数据写入mysql中去 向mysql中写入数据
    val conf = new SparkConf().setMaster("local[2]").setAppName("bbb")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck2")
    val sourceStream = ssc.socketTextStream("master", 9999)

    sourceStream.flatMap(_.split("\\W+"))
      .map((_, 1))
      // .reduceByKey(_+_)
      .updateStateByKey((seq1: Seq[Int], opt1: Option[Int]) => {
        Some(seq1.sum + opt1.getOrElse(0))
      })
      .foreachRDD(rdd => {
        //先创建sparkSession
        val spark = SparkSession.builder()
          .config(rdd.sparkContext.getConf)
          .getOrCreate()
        //转换
        import spark.implicits._
        val df = rdd.toDF("word", "count")

        //写
        //df.write.mode("append").jdbc("jdbc:mysql://master:3306/mydb666","word1015",pros)
        //更新部分 上面的reduceByKey 换成 updateStateByKey ;这里append换成overwrite
        df.write.mode("overwrite").jdbc("jdbc:mysql://master:3306/mydb666", "word1015", pros)

      })

    ssc.start()
    ssc.awaitTermination()
  }

}
