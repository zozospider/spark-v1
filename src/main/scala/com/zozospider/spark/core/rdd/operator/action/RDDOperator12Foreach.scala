package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子 - foreach()
object RDDOperator12Foreach {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // foreach():
    // 分布式遍历 RDD 中的每一个元素，调用指定函数

    // Scala 集合的方法和 RDD 的方法 (算子) 和不一样:
    //   Scala 集合的方法是在同一个节点的内存中执行
    //   RDD 的方法 (算子) 可以将计算逻辑发送到 Executor 端 (分布式节点) 执行
    //   RDD 的方法 (算子) 外部的操作在 Driver 端执行, 内部的操作在 Executor 端 (分布式节点) 执行

    val rdd: RDD[Int] = context.makeRDD(List(10, 2, 3, 4))

    // 此处 foreach 其实是 Driver 端获取到所有 Executor 端 (分布式节点) 执行的返回结果 Array 后, 再在 Driver 端执行 Array 打印的
    rdd.collect.foreach(println)

    println("------")

    // 此处 foreach 其实是 Executor 端 (分布式节点) 分别执行打印的
    rdd.foreach(println)

    context.stop
  }

}
