package com.com.atguigu.test

/**
 * @author hpf
 * @create 2021/6/17 下午5:59  
 * @Version 1.0
 */
object test2 extends APP with Dao {
  def main(args: Array[String]): Unit = {
    //    val ball = new MyBall
    //  println(ball.describe())
    login(new User("uzi",27))

    println("for 推倒式")
    for {
      i <- 1 to 3
      j = 4 - i
    }{
      println("i="+i+" j="+j)
    }

    //
    for(i <- 1 to 3; j = 4 - i) {
      println("i=" + i + " j=" + j)
    }
  }
}



trait Ball {
  //特质中具体的方法
  def describe(): String = {
    "ball"
  }
}

trait Color extends Ball {
  override def describe(): String = {
    "blue-" + super.describe()
  }
}

trait Category extends Ball {
  override def describe(): String = {
    "foot-" + super[Ball].describe()
  }
}

class MyBall extends Category with Color {
  override def describe(): String = {
    "my ball is a " + super.describe()
  }
}


//////////////
case class User(name: String, age: Int)

trait Dao {
  def insert(user: User) = {
    println("insert into database :" + user.name)
  }
}

trait APP {
  _: Dao =>

  def login(user: User) = {
    println("login: " + user.name)
    insert(user)
  }
}