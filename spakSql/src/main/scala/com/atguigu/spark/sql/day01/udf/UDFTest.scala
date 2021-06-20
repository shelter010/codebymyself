package com.atguigu.spark.sql.day01.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
 * @author hpf
 * @create 2021/4/13 下午12:15  
 * @Version 1.0
 */
object UDFTest {
  def main(args: Array[String]): Unit = {
    //子主要是自定义聚合函数
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("udfDemo")
      .getOrCreate()

    //注册自定义函数
    spark.udf.register("myAvg", new MyAvg123)
    val df = spark.read.json(ClassLoader.getSystemResource("user.json").getPath)
    df.createOrReplaceTempView("user")
    spark.sql("select myAvg(age),name from user").show
  }
}

//自定义聚合函数 对age聚合
class MyAvg123 extends UserDefinedAggregateFunction {
  //返回聚合1函数输入数据的类型 是double
  override def inputSchema: StructType = StructType(StructField("inputColumn", DoubleType) :: Nil)

  //聚合缓冲区中值的类型
  override def bufferSchema: StructType = StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)

  //最终返回值类型
  override def dataType: DataType = DoubleType

  //确定性 比如同样的输入是否返回同样的输出
  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //存储数据总和
    buffer(0) = 0D
    //存储数据个数
    buffer(1) = 0L
  }

  //分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  //分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
     if (!buffer2.isNullAt(0)){
       buffer1(0) = buffer1.getDouble(0)+buffer2.getDouble(0)
       buffer1(1) = buffer1.getLong(1)+buffer2.getLong(1)
     }
  }
//计算最终结果 因为是聚合函数 所以最后只有一行
  override def evaluate(buffer: Row): Any ={
    println(buffer.getDouble(0),buffer.getLong(1))
    buffer.getDouble(0) / buffer.getLong(1)
  }
}
