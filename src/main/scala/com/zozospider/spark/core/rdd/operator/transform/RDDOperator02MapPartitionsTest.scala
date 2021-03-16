package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - mapPartitions() - test - 获取每个数据分区的最大值
object RDDOperator02MapPartitionsTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)

    val rdd2: RDD[Int] = rdd.mapPartitions((iterator: Iterator[Int]) => {
      println(">>>")
      List(iterator.max).iterator
    })

    rdd2.collect.foreach(println)

    context.stop
  }

}
