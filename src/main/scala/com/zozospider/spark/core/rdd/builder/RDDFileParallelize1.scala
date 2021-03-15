package com.zozospider.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 文件 RDD 的并行度 & 分区: 分区数据的分配
object RDDFileParallelize1 {

  def main(args: Array[String]): Unit = {

    // 准备环境
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 创建 RDD
    // 数据分区的分配

    // 1. 数据以行为单位进行读取: Spark 读取文件采用 Hadoop 的方式一行一行读取, 和字节数无关

    // 2. 数据读取以偏移量为单位
    // 例 data-dir/11.txt (7 个字节) (## 表示回车占用两个字节):
    // 数据    偏移量
    // a## -> 000102
    // b## -> 030405
    // c   -> 06

    // 3. 数据分区偏移量范围的计算
    // 分区               偏移量数据范围
    // part-00000 分区 -> [00, 03] (a##b)
    // part-00001 分区 -> [03, 06] (c)
    // part-00002 分区 -> [06, 07]
    val rdd: RDD[String] = context.textFile(path = "data-dir\\11.txt", minPartitions = 2)

    // 处理结果保存成分区文件
    rdd.saveAsTextFile("output")

    // 关闭环境
    context.stop
  }

}
