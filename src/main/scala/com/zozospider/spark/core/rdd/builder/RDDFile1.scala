package com.zozospider.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 从外部存储 (如文件) 中创建 RDD
object RDDFile1 {

  def main(args: Array[String]): Unit = {

    // 准备环境
    // local[*] 表示采用的线程数和当前机器的核数相同
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 创建 RDD
    // textFile: 以行为单位读取数据, 读取的数据是字符串
    // wholeTextFiles: 以文件为单位读取数据, 读取的结果表示为元组, 第一个元素为文件路径, 第二个元素为文件内容
    val rdd: RDD[(String, String)] = context.wholeTextFiles("data-dir")

    // 采集并打印结果
    rdd.collect.foreach(println)

    // 关闭环境
    context.stop
  }

}
