package com.atguigu.spark.sql.day01

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author hpf
 * @create 2021/2/26 上午7:33  
 * @Version 1.0
 */
object RDD2DF2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("ha1").getOrCreate()
    val sc = spark.sparkContext

    val rdd1 = sc.parallelize(("lisi", 12) :: ("zhangsan", 22) :: Nil)
      .map({
        case (name,age) => Row(name,age)
      })
    val schema = StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))
    val df = spark.createDataFrame(rdd1, schema)
    df.show()
    spark.close()
  }
}
