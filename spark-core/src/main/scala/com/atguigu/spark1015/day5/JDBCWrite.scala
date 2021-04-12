package com.atguigu.spark1015.day5

import java.sql.DriverManager

import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/19 上午8:22  
 * @Version 1.0
 */
object JDBCWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DbcWrite").setMaster("local[2]")
    val sc = new SparkContext(conf)
   // new HadoopRDD[]()
    val url = "jdbc:mysql://master:3306/mydb666"
    val user = "root"
    val passwd = "123456"

    //先有数据 然后去写
    val rdd1 = sc.parallelize((20, "hpf") :: (25, "heimao") :: (30, "dabai") :: Nil)
  //  Class.forName("com.mysql.jdbc.Driver")

   val sql = "insert into hpf1 values(?,?)"
//    rdd1.foreach {
//
//      case (age, name) => {
//        val conn = DriverManager.getConnection(url, user, passwd)
//
//        val ps = conn.prepareStatement(sql)
//        ps.setInt(1, age)
//        ps.setString(2, name)
//        ps.execute()
//        ps.close()
//        conn.close()
//
//      }
//    }

    //下面是改进之后合理的做法 用foreachPartition来做1 每个1partition建立一个连接1
//    rdd1.foreachPartition(it =>{
//      Class.forName("com.mysql.jdbc.Driver")
//      val conn = DriverManager.getConnection(url,user,passwd)
//      it.foreach{
//        case (age,name) =>{
//          val ps = conn.prepareStatement(sql)
//          ps.setInt(1,age)
//          ps.setString(2,name)
//          ps.execute()
//          ps.close()
//
//        }
//      }
//      conn.close()
//    })
    //再次改进 按照1批次1提交1
    rdd1.foreachPartition(it =>{
      Class.forName("com.mysql.jdbc.Driver")
      val connection = DriverManager.getConnection(url,user,passwd)
      val statement = connection.prepareStatement(sql)
      var count = 0
      it.foreach({
        case (age,name) =>{
          statement.setInt(1,age)
          statement.setString(2,name)
          statement.addBatch()
          count += 1
          if (count % 100 == 0) statement.executeBatch()
        }
      })
      //bi xu yao you
      statement.executeBatch()
      connection.close()
    })
   sc.stop()
  }
}
