package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - mapPartitionsWithIndex()
object RDDOperator03MapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // mapPartitionsWithIndex():
    // 将待处理的数据以分区为单位发送到计算节点进行处理, 这里的处理是指可以进行任意的处理, 哪怕是过滤数据, 在处理时同时可以获取当前分区索引

    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)

    val rdd2: RDD[Int] = rdd.mapPartitionsWithIndex((index: Int, iterator: Iterator[Int]) => {
      println(s"index = $index")
      iterator
    })

    rdd2.collect

    context.stop
  }

}
