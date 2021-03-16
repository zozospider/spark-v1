package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - filter()
object RDDOperator07Filter {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // filter():
    // 将数据根据指定的规则进行筛选过滤, 符合规则的数据保留, 不符合规则的数据丢弃
    // 当数据进行筛选过滤后, 分区不变, 但是分区内的数据可能不均衡, 生产环境下, 可能会出现数据倾斜

    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))

    // 保留奇数
    val rdd2: RDD[Int] = rdd.filter((i: Int) => i % 2 != 0)

    rdd2.collect.foreach(println)

    context.stop
  }

}
