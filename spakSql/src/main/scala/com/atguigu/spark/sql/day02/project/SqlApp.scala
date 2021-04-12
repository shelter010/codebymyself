package com.atguigu.spark.sql.day02.project

import java.text.DecimalFormat
import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

/**
 * @author hpf
 * @create 2021/2/27 下午4:25  
 * @Version 1.0
 */
object SqlApp {
  // System.setProperty("HADOOP_USER_NAME","root")
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HiveWrite4")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    spark.udf.register("remark", new MyRemarkUDAF)
    //去执行 sql  从hive查数据
    spark.sql("use spark1018")
    //    spark.sql("select city_name from city_info").show()
    spark.sql(
      """
        |select
        |    ci.*,
        |    pi.product_name,
        |    uva.click_product_id
        |from user_visit_action uva
        |join product_info pi on uva.click_product_id = pi.product_id
        |join city_info ci on uva.city_id = ci.city_id
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        |    ifNull(area,"0") area,
        |    product_name,
        |    count(*)  count,
        |    remark(city_name) remark
        |from t1
        |group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")
    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    count,
        |    remark,
        |    rank() over(partition by area order by count desc) rk
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")

    val pros = new Properties()
    pros.setProperty("user", "root")
    pros.setProperty("password", "123456")
    spark.sql(
      """
        |select
        |   area,
        |   product_name,
        |   count,
        |   remark
        |from t3
        |where rk <= 3
        |""".stripMargin)
      .coalesce(1)
      .write
      .mode("overwrite")
      //解决乱码问题
      .jdbc("jdbc:mysql://master:3306/mydb666", "sql10177", pros)

    //把结果写入到mysql中 看上面 代码


    spark.close()
  }

}

//自定义聚合函数
class MyRemarkUDAF extends UserDefinedAggregateFunction {
  //数据输入的类型 北京 String
  override def inputSchema: StructType = StructType(Array(StructField("city", StringType)))

  //buffer缓存类型
  override def bufferSchema: StructType = StructType(Array(StructField("map", MapType(StringType, LongType)),
    StructField("total", LongType)))

  //最终聚合类型  beijing 21.2% tianjin13.2% qita65.6% String
  override def dataType: DataType = StringType

  //确定性
  override def deterministic: Boolean = true

  //缓存器初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  //分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    input match {
      //1 总的点击量 + 1
      case Row(cityName: String) =>
        buffer(1) = buffer.getLong(1) + 1L
        //2 给这个城市的点击量+1 =》 找到缓存map 取出这个城市原来的 加+1 再赋值过去
        val map = buffer.getMap[String, Long](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
      case _ =>
    }


  }

  //分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getMap[String, Long](0)
    val map2 = buffer2.getMap[String, Long](0)

    val total1 = buffer1.getLong(1)
    val total2 = buffer2.getLong(1)
    //1 总数的聚合
    buffer1(1) = total1 + total2
    //2 map的聚合
    buffer1(0) = map1.foldLeft(map2) {
      case (map, (cityName, count)) =>
        map + (cityName -> (map.getOrElse(cityName, 0L) + count))
    }
  }

  //最后的输出结果
  override def evaluate(buffer: Row): Any = {
    //  北京 21。2% 天津 13。2%  其他 65。6%
    val cityAndCount = buffer.getMap[String, Long](0)
    val total = buffer.getLong(1)

    val cityAndCountTop2 = cityAndCount.toList.sortBy(-_._2).take(2)

    var cityRemarks = cityAndCountTop2.map {
      case (cityName, count) => CityRemark(cityName, count.toDouble / total)
    }

    cityRemarks :+= CityRemark("其他", cityRemarks.foldLeft(1D)(_ - _.d))
    cityRemarks.mkString(",")

  }
}

case class CityRemark(cityName: String, d: Double) {
  private val f = new DecimalFormat("0.00%")

  override def toString: String = s"$cityName:${f.format(d)}"
}


/*
各区域热门商品top3

这里的热门商品是从点击量的维度来看的.
计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。
例如:
地区	商品名称		点击次数	城市备注
华北	商品A		100000	北京21.2%，天津13.2%，其他65.6%
华北	商品P		80200	北京63.0%，太原10%，其他27.0%
华北	商品M		40000	北京63.0%，太原10%，其他27.0%
东北	商品J		92000	大连28%，辽宁17.0%，其他 55.0%

-------
自定义聚合函数
-------
user_visit_action product_info  city_info
1 先把需要的字段查出来  t1
select
    ci.*,
    pi.product_name,
    uva.click_product_id
from user_visit_action uva
join product_info pi on uva.click_product_id = pi.product_id
join city_info ci on uva.city_id on ci.city_id;

2按照地区和商品名称聚合  t2
select
    area,
    product_name,
    count(*)  count
from t1
group by area,product_name

3 按照地区进行分组开窗 排序 添加一个(rank(1 2 2 4 4) row_number(1 2 3 4)  dense_rank(1 2 2 3 4))
//t3
select
    area,
    produce_name,
    count,
    rank() over(partition by area order by count desc) rk
from t2

4 过滤出来名次小于等于3的
select
   area,
   product_name,
   count
from t3
where rk <= 3

 */