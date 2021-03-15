package com.zozospider.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 内存 RDD 的并行度 & 分区: 分区的设定
object RDDMemoryParallelize {

  def main(args: Array[String]): Unit = {

    // 准备环境
    // local[*] 表示采用的线程数和当前机器的核数相同
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    // RDD 未指定 numSlices (RDD 的并行度 & 分区) 时可用
    conf.set("spark.default.parallelism", "5")
    val context: SparkContext = new SparkContext(conf)

    // 创建 RDD
    // numSlices: 分区数量 (RDD 的并行度 & 分区)
    // numSlices 默认为 defaultParallelism, 本地环境取配置 spark.default.parallelism, 如果没有配置则取 totalCores (当前环境的核数)
    // class LocalSchedulerBackend{
    //  override def defaultParallelism(): Int =
    //      scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // }
    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4))
    // val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)

    // 处理结果保存成分区文件
    rdd.saveAsTextFile("output")

    // 关闭环境
    context.stop
  }

}
