package com.atguigu.spark1015.day5

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/20 上午7:58  
 * @Version 1.0
 */
object HbaseRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("HbaseRead")
    val sc = new SparkContext(conf)
   //从HBase中读取数据

    // 1 连接hbase配置
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum","master,slave1,slave2")
    configuration.set(TableInputFormat.INPUT_TABLE,"staff2")

    //从HBase读取数据
    val rdd1 = sc.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat], //rowKey封装在这个类型中
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    //读到数据封装下
    val resultRdd = rdd1.map(
      {
        case (iw, result) => {
          Bytes.toString(iw.get())
        }
      }
    )
    resultRdd.collect().foreach(println)
    sc.stop()
  }
}
