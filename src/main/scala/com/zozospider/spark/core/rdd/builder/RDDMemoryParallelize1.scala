package com.zozospider.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 内存 RDD 的并行度 & 分区: 分区数据的分配
object RDDMemoryParallelize1 {

  def main(args: Array[String]): Unit = {

    // 准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 创建 RDD
    // 分区规则主要由源码 ParallelCollectionRDD.slice() 决定
    // 分区结果为: [1, 2], [3, 4]
    // val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)
    // 分区结果为: [1], [2], [3, 4]
    // val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 3)
    // 分区结果为: [1], [2, 3], [4, 5]
    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4, 5), numSlices = 3)

    // 处理结果保存成分区文件
    rdd.saveAsTextFile("output")

    // 关闭环境
    context.stop
  }

}
