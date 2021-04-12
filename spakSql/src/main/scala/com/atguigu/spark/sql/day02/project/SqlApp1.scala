package com.atguigu.spark.sql.day02.project

import java.text.DecimalFormat

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

/**
 * @author hpf
 * @create 2021/2/27 下午11:02  
 * @Version 1.0
 */
object SqlApp1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SqlApp1")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    spark.udf.register("remark1",new MyUDAF)
    spark.sql("use spark1018")
    spark.sql(
      """
        |select
        |            ci.*,
        |            pi.product_name,
        |            uv.click_product_id
        |            from user_visit_action uv
        |         join city_info ci on uv.city_id = ci.city_id
        |         join product_info pi on uv.click_product_id = pi.product_id
        |""".stripMargin).createOrReplaceTempView("t1")
    spark.sql(
      """
        |select
        |          area,
        |          product_name,
        |          count(*) count,
        |          remark1(city_name) re
        |      from t1
        |      group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")
    spark.sql(
      """
        |select
        |          area,
        |          product_name,
        |          count,
        |          re,
        |          rank() over(partition by area order by count desc) rk
        |      from t2
        |
        |""".stripMargin).createOrReplaceTempView("t3")
    spark.sql(
      """
        |select
        |       area `区域`,
        |       product_name `商品名称`,
        |       count `访问次数`,
        |       re
        |     from t3
        |     where rk <= 3
        |""".stripMargin).show(1000, false)

    spark.close()
    //计算地区热门商品 top3   热门商品是从点击量的维度来说的
    //期待的结果是  area 商品 count 排行比率
    /*
      期待结果是：
      地区	商品名称		点击次数	城市备注
华北	商品A		100000	北京21.2%，天津13.2%，其他65.6%
华北	商品P		80200	北京63.0%，太原10%，其他27.0%
华北	商品M		40000	北京63.0%，太原10%，其他27.0%
东北	商品J		92000	大连28%，辽宁17.0%，其他 55.0%
     */
    /*
    分析：
    读取hive中的数据 然后求解
    三张表 商品表 product_info ；用户行为表 user_visit_action ；城市表 city_info
    1 将三张表聚合  得到 area product_id click_product_id（商品点击次数） 通过聚合函数 计算 城市备注 先把前3列写出来

    2 步骤
      2.1  聚合三张表  t1
         select
            ci.*,
            pi.product_name,
            uv.click_product_id
            from user_visit_action uv
         join city_info ci on uv.city_id = ci.city_id
         join product_info pi on uv.click_product_id = pi.product_id
      2.2  按照地区和商品名称聚合 并count同一地地区 同一产品的点击总量  t2
      select
          area,
          product_name,
          count(*) count
      from t1
      group by area,product_name
      2.3  按照地区进行分组开窗 按照数量排序 rank over  t3
      select
          area,
          product_name,
          count,
          rank() over(partition by area order by count desc) rk
      from t2

      2.4 过滤出来名次 小于或者等于3的

     select
       area `区域`,
       product_name `商品名称`,
       count `点击次数`
     from t3 where rk <= 3

     */


  }

}

class MyUDAF extends UserDefinedAggregateFunction {
  //输入是city_name 输出是城市备注的那3个结果
  override def inputSchema: StructType = StructType(Array(StructField("cityName",StringType)))

  override def bufferSchema: StructType = StructType(Array(StructField("city2RadioMap",MapType(StringType,LongType)),StructField("total",LongType)))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //map是缓存器 可以用来存储一些数据
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  //分区间聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //因为输入传的值只有一个
    input match {
        //总的点击加1
      case Row(cityName:String) =>
        buffer(1) = buffer.getLong(1) + 1L
        //找到缓存map 加1 再赋值
        val map = buffer.getMap[String, Long](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName,0L)+1L))
      case _ =>
    }
  }
   //分区间的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getMap[String, Long](0)
    val total1 = buffer1.getLong(1)
    val map2 = buffer2.getMap[String, Long](0)
    val total2 = buffer2.getLong(1)
    //总数的聚合
    buffer1(1) = total1 + total2
    //map的聚合  可以用foldLft方法
    buffer1(0) = map1.foldLeft(map2){
      case (map,(cityName,count)) =>
        map + (cityName -> (map.getOrElse(cityName,0L)+count))
    }



  }

  override def evaluate(buffer: Row): Any = {
    val cityAndCount1 = buffer.getMap[String, Long](0)
    val total = buffer.getLong(1)
    //排序 需要的结果是    //  北京 21。2% 天津 13。2%  其他 65。6% 需要找到top2
    val cityAndCountTop22 = cityAndCount1.toList.sortBy(-_._2).take(2)
    //封装到样例中
    var cityRemarks = cityAndCountTop22.map {
      case (cityName, count) => RemarkRadio(cityName, count.toDouble / total)
    }

    //还有一个其他
    cityRemarks :+= RemarkRadio("其他", cityRemarks.foldLeft(1D)(_-_.d))
    cityRemarks.mkString(",")
  }
}

case class RemarkRadio(cityName: String, d: Double){
  private val f = new DecimalFormat("0.00%")
  override def toString: String = s"$cityName:${f.format(d)}"
}
