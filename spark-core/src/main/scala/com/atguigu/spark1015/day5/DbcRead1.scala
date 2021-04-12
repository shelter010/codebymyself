package com.atguigu.spark1015.day5

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.Serialization

import scala.collection.mutable

/**
 * @author hpf
 * @create 2021/2/20 下午8:20  
 * @Version 1.0
 */
object DbcRead1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DbcRead1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //HBase连接设置
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","master,slave1,slave2")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"staff2")

    //读取hbase数据
    val rdd1 = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    val rdd2 = rdd1.map({
      case (iw,result) =>{
        val map = mutable.Map[String, Any]()
        //首先将rowKey放到map中
        map += "rowKey" -> Bytes.toString(iw.get())
        //再把每一列 也放到map中
        val cells = result.listCells()
        import scala.collection.JavaConversions._
        for (cell <- cells){
         // 列名 -》 列值
         val key = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          map += key -> value
        }
    //    map
        //把map转换成json json4s（json4scala）
       implicit val df = org.json4s.DefaultFormats
        Serialization.write(map)
      }
    })

    rdd2.collect().foreach(println)
    sc.stop()
  }
}
