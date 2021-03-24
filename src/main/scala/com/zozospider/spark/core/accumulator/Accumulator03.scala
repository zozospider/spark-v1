package com.zozospider.spark.core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

// 累加器 - 问题
object Accumulator03 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Accumulator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 少加: 转换算子 (map()) 中调用累加器, 如果没有行动算子 (collect()) 就不会执行, 结果有问题
    val rddA: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))
    val accumulatorA: LongAccumulator = context.longAccumulator
    rddA.map((i: Int) => {
      accumulatorA.add(i)
      accumulatorA
    })
    println(accumulatorA.value)

    println("------")

    // 多加: 转换算子 (map()) 中调用累加器, 有行动算子 (collect()) 但是调用多次, 结果有问题
    val rddB: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))
    val accumulatorB: LongAccumulator = context.longAccumulator
    val rddB2: RDD[LongAccumulator] = rddB.map((i: Int) => {
      accumulatorB.add(i)
      accumulatorB
    })
    rddB2.collect
    rddB2.collect
    println(accumulatorB.value)

    context.stop
  }

}
