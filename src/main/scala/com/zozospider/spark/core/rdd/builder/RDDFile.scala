package com.zozospider.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 从外部存储 (如文件) 中创建 RDD
object RDDFile {

  def main(args: Array[String]): Unit = {

    // 准备环境
    // local[*] 表示采用的线程数和当前机器的核数相同
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 创建 RDD
    // 从文件中创建 RDD, 将文件中集合的数据作为处理的数据源
    // path 可以是 绝对路径 / 相对路径 (以当前环境的根路径为基准)
    // path 可以是 文件 / 文件夹
    // path 可以使用通配符
    // path 可以是分布式存储系统路径, 如 HDFS
    // val rdd: RDD[String] = context.textFile("D:\\zz\\code\\idea\\spark-v1\\data-dir")
    // val rdd: RDD[String] = context.textFile("data-dir\\1.txt")
    // val rdd: RDD[String] = context.textFile("data-dir\\1*.txt")
    // val rdd: RDD[String] = context.textFile("hdfs://linux1:8020/test")
    val rdd: RDD[String] = context.textFile("data-dir")

    // 采集并打印结果
    rdd.collect.foreach(println)

    // 关闭环境
    context.stop
  }

}
