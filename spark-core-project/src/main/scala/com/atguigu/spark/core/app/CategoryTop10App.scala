package com.atguigu.spark.core.app

import com.atguigu.spark.core.acc.CategoryAcc1
import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author hpf
 * @create 2021/2/24 下午8:16  
 * @Version 1.0
 */
object CategoryTop10App {
  def statCategoryTop10(sc: SparkContext, userVisitActionRdd: RDD[UserVisitAction]): Array[CategoryCountInfo] = {
    // 主函数调用这个方法 实现累加
    //通过累加器来实现 计算 每个品类的 点击量 下单量 支付数量
   //累加的IN 实时UserVisitAction OUT可以是 map类型 组成2 map[(pin lei,"click"),count ]...
    val acc = new CategoryAcc1
    sc.register(acc)
    userVisitActionRdd.foreach(action => acc.add(action))
    //acc.value.foreach(println)
    //上面是注册累加器

    //1 把1个品类的3个指标封装到1个map中
    val categoryCountGrouped = acc.value.groupBy(_._1._1)

    //把结果封装到样例类中
    val categoryInfoArray = categoryCountGrouped.map({
      case (cid, map) => {
        CategoryCountInfo(
          cid,
          map.getOrElse((cid, "click"), 0L),
          map.getOrElse((cid, "order"), 0L),
          map.getOrElse((cid, "pay"), 0L)
        )
      }
    }).toArray

    //3 对数据进行排序取top10
    val result = categoryInfoArray.sortBy(into => (-into.clickCount, -into.orderCount, -into.payCount))
      .take(10)

    //result.foreach(println)
    result
  }

}
