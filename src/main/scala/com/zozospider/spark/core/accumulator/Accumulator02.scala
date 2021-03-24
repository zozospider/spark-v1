package com.zozospider.spark.core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

// 累加器 - 系统累加器
object Accumulator02 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Accumulator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 累加器: 分布式共享只写变量

    // 累加器用来把 Executor 端变量信息聚合到 Driver 端
    // 在 Driver 程序中定义的变量, 在 Executor 端的每个 Task 都会得到这个变量的一份新的副本
    // 每个 task 更新这些副本的值后, 传回 Driver 端进行 merge

    // 使用累加器, Driver 端的 sum 会被传到 Executor 端分别计算, 计算完成后传回到 Driver 端进行合并计算

    // 使用累加器可以在某些场景下避免 shuffle 操作, 提高性能 (如此处可以使用 rdd.reduce(_ + _) 但是会进行 shuffle 操作)

    // 系统累加器: LongAccumulator, DoubleAccumulator, CollectionAccumulator (内部存储是 List)

    // 注: 必须最终有行动算子 (如 foreach(), collect() 等) 才会出发累加器执行

    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))

    val accumulator: LongAccumulator = context.longAccumulator
    rdd.foreach((i: Int) => accumulator.add(i))
    println(accumulator.value)

    context.stop
  }

}
