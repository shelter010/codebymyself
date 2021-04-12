package com.atguigu.data.mock.bean

/**
 * @author hpf
 * @create 2021/3/1 下午10:08  
 * @Version 1.0
 */
object CityInfo {
  /**
   * 城市表
   *
   * @param city_id   城市 id
   * @param city_name 城市名
   * @param area      城市区域
   */
  case class CityInfo(city_id: Long,
                      city_name: String,
                      area: String)

}
