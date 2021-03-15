package com.zozospider.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 从内存中创建 RDD
object RDDMemory {

  def main(args: Array[String]): Unit = {

    // 准备环境
    // local[*] 表示采用的线程数和当前机器的核数相同
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 创建 RDD
    // 从内存中创建 RDD, 将内存中集合的数据作为处理的数据源
    val seq: Seq[Int] = Seq(1, 2, 3, 4)
    // parallelize: 并行
    // val rdd: RDD[Int] = context.parallelize(seq)
    // makeRDD: 调用的就是 parallelize 方法
    val rdd: RDD[Int] = context.makeRDD(seq)

    // 采集并打印结果
    rdd.collect.foreach(println)

    // 关闭环境
    context.stop
  }

}
