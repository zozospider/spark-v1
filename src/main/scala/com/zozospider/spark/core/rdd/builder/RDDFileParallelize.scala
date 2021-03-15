package com.zozospider.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 文件 RDD 的并行度 & 分区: 分区的设定
object RDDFileParallelize {

  def main(args: Array[String]): Unit = {

    // 准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 创建 RDD
    // Spark 分区设定方式, 底层使用的是 Hadoop 的 TextInputFormat.java, 主要由源码 FileInputFormat.getSplits() 决定
    // minPartitions: 最小分区数量, 真正的分区数可能比最小分区数要大
    // minPartitions 默认为 defaultMinPartitions, 取 defaultParallelism 和 2 的最小值 (其中 defaultParallelism 见 RDDMemoryParallelize.scala 中的说明)
    // class SparkContext{
    //  def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
    // }
    // val rdd: RDD[String] = context.textFile(path = "data-dir\\11.txt")
    val rdd: RDD[String] = context.textFile(path = "data-dir\\11.txt", minPartitions = 2)

    // 处理结果保存成分区文件
    rdd.saveAsTextFile("output")

    // 关闭环境
    context.stop
  }

}
