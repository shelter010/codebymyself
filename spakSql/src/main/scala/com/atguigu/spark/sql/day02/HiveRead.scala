package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/2/27 下午1:44  
 * @Version 1.0
 */
object HiveRead {
  def main(args: Array[String]): Unit = {
    //外置hive 访问读取
    val spark = SparkSession
      .builder()
      .appName("HiveRead")
      .master("local[*]")
      //添加支持外置hive 同时需要添加spark对hive
      //的支持的jar包到pom.xml文件中  如果还有问题
      //把core-site.xml he hdfs-site.xml复制过来
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //这种方式 使用的内置hive 需要加上 上面代码
    spark.sql("show databases")
    spark.sql("use gmall")
    spark.sql("select count(*) from ads_uv_count").show()

    spark.close()
  }
}
