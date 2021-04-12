package com.atguigu.spark.core.app

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/24 下午8:07  
 * @Version 1.0
 *          计算每个品类的点击量 下单量 以及支付量 并返回
 */
object CategoryTopApp {
  //代码的执行部分
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ha")
    val sc = new SparkContext(conf)
    val sourceRdd = sc.textFile("/Users/mac/Downloads/download from mac/download from baiduwangpan/15-spark/spark-core数据/user_visit_action.txt")
    //将数据封装到样例类中
    val userVisitActionRdd = sourceRdd.map(line => {
      val splits = line.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong)
    })
 //需求1
//     val categoryTop10 = CategoryTop10App.statCategoryTop10(sc,userVisitActionRdd)
//     //CategorySessionTop10.statSessionTop10(sc,categoryTop10,userVisitActionRdd)
//    CategorySessionTop11.statSessionTop11(sc,categoryTop10,userVisitActionRdd)
//    //需求2 计算top10的session 点击最多的
//    CategorySessionTop12.statSessionTop12(sc,categoryTop10,userVisitActionRdd)
//
  PageConversion.statPageConversionRate(sc,userVisitActionRdd,"1,2,3,4,5,6,7")
  //关闭项目
    sc.stop()
  }

}
