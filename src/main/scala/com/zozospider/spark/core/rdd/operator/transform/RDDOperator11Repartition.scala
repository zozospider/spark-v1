package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - repartition()
object RDDOperator11Repartition {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // repartition():
    // 该操作内部其实执行的是 coalesce 操作, 参数 shuffle 的默认值为 true.
    // 无论是缩减分区还是扩大分区, repartition 操作都可以完成, 因为无论如何都会 shuffle

    // repartition 调用的就是 coalesce(numPartitions, shuffle = true)

    // 缩减分区: coalesce(numPartitions)
    // 缩减分区 & shuffle: coalesce(numPartitions, shuffle = true)
    // 缩减分区 & shuffle: repartition(numPartitions)

    // 扩大分区 & shuffle: coalesce(numPartitions, shuffle = true)
    // 扩大分区 & shuffle: repartition(numPartitions)

    // val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4, 5, 6), numSlices = 6)
    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4, 5, 6), numSlices = 2)

    // val rdd2: RDD[Int] = rdd.repartition(numPartitions = 2)
    val rdd2: RDD[Int] = rdd.repartition(numPartitions = 3)

    rdd2.saveAsTextFile("output")

    context.stop
  }

}
