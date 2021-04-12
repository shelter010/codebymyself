package com.atguigu.spark.core.app

import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author hpf
 * @create 2021/2/24 下午10:57  
 * @Version 1.0
 */
object CategorySessionTop11 {
  def statSessionTop11(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRdd: RDD[UserVisitAction]): Unit = {
    val categoryIdRdd = categoryTop10.map(_.categoryId.toLong)
    //categoryId he clickCategoryId 相同
    val filterUserVisitRdd = userVisitActionRdd.filter(action => categoryIdRdd.contains(action.click_category_id))
    val cidSidAndOne = filterUserVisitRdd.map(action => ((action.click_category_id, action.session_id), 1))

    val result = cidSidAndOne.reduceByKey(_ + _)
      .map({
        case ((cid, sid), count) => (cid, (sid, count))
      })
      .groupByKey()
      .mapValues(it => {
        it.toList.sortBy(_._2).takeRight(10)
      })
    //result.collect.foreach(println)
  }   //求出每个品类top10的浏览量的top10
  //坏处 以上有reduceByKey 和reduceByKey 做了2次shuffle 比较消耗性能

    //方案4 对对上面优化 减少1次shffle

}
