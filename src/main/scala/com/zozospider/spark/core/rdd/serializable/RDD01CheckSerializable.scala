package com.zozospider.spark.core.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 序列化 - 闭包检查
object RDD01CheckSerializable {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 从计算的角度, 算子以外的代码都是在 Driver 端执行, 算子里面的代码都是在 Executor 端执行
    // 那么在 scala 的函数式编程中, 就会导致算子内经常会用到算子外的数据, 这样就形成了闭包的效果
    // 如果使用的算子外的数据无法序列化, 就意味着无法传值给 Executor 端执行, 就会发生错误
    // 所以需要在执行任务计算前, 检测闭包内的对象是否可以进行序列化, 这个操作我们称之为闭包检测
    // Scala 2.12 版本后闭包编译方式发生了改变

    // rdd 调用算子方法时, 会在 runJob() 之前进行闭包检查, 所以即使 rdd 中没有数据, 也会检查错误：
    // def foreach(f: T => Unit): Unit = withScope {
    //    val cleanF = sc.clean(f)
    //    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
    //  }
    // val rdd: RDD[Int] = context.makeRDD(List())
    val rdd: RDD[Int] = context.makeRDD(List(10, 2, 3, 4))

    // A1: rdd.foreach() 方法用到了 age 属性, 所以 age 属性需要序列化, 所以 UserA 需要序列化 (UserA 未序列化, 所以会报错)
    // A2: rdd.foreach() 方法没有直接用到 age 属性, 所以 age 属性不需要序列化, 所以 UserA 不需要序列化 (所以不会报错)
    // B: rdd.foreach() 方法用到了 age 属性, 所以 age 属性需要序列化, 所以 UserB 需要序列化 (UserB 有序列化, 所以不会报错)
    // C: rdd.foreach() 方法用到了 age 属性, 所以 age 属性需要序列化, 所以 UserC 需要序列化 (UserC 为 case class, 有序列化, 所以不会报错)

    // 执行时会报异常: org.apache.spark.SparkException: Task not serializable
    val userA1: UserA = new UserA
    rdd.foreach((i: Int) => println(s"userA1.age = ${userA1.age}, i = $i"))

    println("------")

    val userA2: UserA = new UserA
    val a: Int = userA2.age
    rdd.foreach((i: Int) => println(s"a = $a, i = $i"))

    println("------")

    // 正常执行
    val userB: UserB = new UserB
    rdd.foreach((i: Int) => println(s"userB.age = ${userB.age}, i = $i"))

    println("------")

    // 正常执行
    val userC: UserC = new UserC
    rdd.foreach((i: Int) => println(s"userC.age = ${userC.age}, i = $i"))

    context.stop
  }

  class UserA {
    val age: Int = 30
  }

  class UserB extends Serializable {
    val age: Int = 30
  }

  case class UserC() {
    val age: Int = 30
  }

}
