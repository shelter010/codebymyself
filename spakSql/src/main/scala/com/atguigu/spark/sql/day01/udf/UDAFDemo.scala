package com.atguigu.spark.sql.day01.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

import scala.collection.immutable.Nil

/**
 * @author hpf
 * @create 2021/2/26 下午7:50  
 * @Version 1.0
 */
object UDAFDemo {
  //在sql中 聚合函数如何使用
     def main(args: Array[String]): Unit = {
       val spark: SparkSession = SparkSession.builder().master("local[2]").appName("UDAFDemo").getOrCreate()
       //通过 sparkSession 来创建df
       val df1 = spark.read.json("/Users/mac/Downloads/download from mac/download from baiduwangpan/15-spark/spark-sql数据/user.json")

        //创建临时表
       df1.createOrReplaceTempView("emp")
       //注册聚合函数
//       spark.udf.register("mySum",new MySum)
//       spark.sql("select mySum(age) from emp").show()

       //求解age平均值的聚合函数
        spark.udf.register("myAvg",new MyAvg)
       spark.sql("select myAvg(age) from emp").show()
       spark.close()
     }
}

class MySum extends UserDefinedAggregateFunction{
  //输入的数据类型
  override def inputSchema: StructType = StructType(StructField("ele",DoubleType)::Nil)
//缓存区的类型
  override def bufferSchema: StructType = StructType(StructField("ele",DoubleType)::Nil)
//最终聚合结果的类型
  override def dataType: DataType = DoubleType
//相同的输入是否返回相同的输出
  override def deterministic: Boolean = true
//对缓存区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0D //等同于 buffer.update(0,0D)
  }
//分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
   //input 是指的使用聚合函数时候 缓存过来的参数封装在
    if (! input.isNullAt(0)) {

      val v = input.getAs[Double](0) //getDouble(0)
      buffer(0) = buffer.getDouble(0) + v
    }

  }
//分区间合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //把buffer1 和 buffer2 中的缓存聚在一起 然后把值写回到buffer1
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
  }
//返回最终的输出值
  override def evaluate(buffer: Row): Any = buffer.getDouble(0)
}

class MyAvg extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("sum",DoubleType)::StructField("count",LongType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("sum",DoubleType)::StructField("count",LongType)::Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0D
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (! input.isNullAt(0)) {

      val v = input.getDouble(0)
      buffer(0) = buffer.getDouble(0) + v
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) +  buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = buffer.getDouble(0) / buffer.getLong(1)
}
